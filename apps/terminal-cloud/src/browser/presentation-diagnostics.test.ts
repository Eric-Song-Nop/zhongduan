import type { BrowserTelemetryEvent } from "@zhongduan/telemetry";
import { describe, expect, it, vi } from "vitest";

import {
  createBrowserPresentationDiagnostics,
  resolveBrowserDiagnosticsMode,
  type BrowserCanonicalPresentationProbe,
  type BrowserInputKind,
  type BrowserInputLifecycleProbe,
  type BrowserPresentationDiagnostics,
  type BrowserPresentationDiagnosticsOptions,
} from "./presentation-diagnostics";

interface ScheduledCallback {
  cancelled: boolean;
  callback: () => void;
  delayMs: number;
}

interface ScheduledFrame {
  callback: (timestamp: number) => void;
  cancelled: boolean;
}

function createHarness(
  overrides: Partial<BrowserPresentationDiagnosticsOptions> = {},
  phases: readonly number[] = [0, 0],
) {
  let now = 0;
  let nextHandle = 1;
  let phaseIndex = 0;
  const events: BrowserTelemetryEvent[] = [];
  const timers = new Map<number, ScheduledCallback>();
  const frames = new Map<number, ScheduledFrame>();
  const setTimer = vi.fn((callback: () => void, delayMs: number) => {
    const handle = nextHandle++;
    timers.set(handle, { callback, delayMs, cancelled: false });
    return handle;
  });
  const clearTimer = vi.fn((handle: unknown) => {
    const timer = timers.get(handle as number);
    if (timer !== undefined) timer.cancelled = true;
  });
  const requestAnimationFrame = vi.fn((callback: (timestamp: number) => void) => {
    const handle = nextHandle++;
    frames.set(handle, { callback, cancelled: false });
    return handle;
  });
  const cancelAnimationFrame = vi.fn((handle: unknown) => {
    const frame = frames.get(handle as number);
    if (frame !== undefined) frame.cancelled = true;
  });
  const options: BrowserPresentationDiagnosticsOptions = {
    telemetry: (event) => events.push(event),
    monotonicNow: () => now,
    randomUint32: () => phases[phaseIndex++] ?? 0,
    setTimer,
    clearTimer,
    requestAnimationFrame,
    cancelAnimationFrame,
    ...overrides,
  };
  const tracker = createBrowserPresentationDiagnostics("memory-v2", options);
  if (tracker === undefined) throw new Error("memory-v2 tracker was disabled");

  return {
    cancelAnimationFrame,
    clearTimer,
    events,
    frames,
    requestAnimationFrame,
    setNow(value: number) {
      now = value;
    },
    setTimer,
    timers,
    tracker,
    invokeActiveFrame(timestamp = now) {
      const entry = [...frames.values()].find((frame) => !frame.cancelled);
      if (entry === undefined) throw new Error("no active animation frame");
      entry.cancelled = true;
      entry.callback(timestamp);
    },
    invokeActiveTimer() {
      const entry = [...timers.values()].find((timer) => !timer.cancelled);
      if (entry === undefined) throw new Error("no active timer");
      entry.cancelled = true;
      entry.callback();
    },
  };
}

function nextSampledInput(
  tracker: BrowserPresentationDiagnostics,
  kind: BrowserInputKind = "key",
): BrowserInputLifecycleProbe {
  for (let attempt = 0; attempt < 64; attempt += 1) {
    const probe = tracker.beginInputDispatch(kind);
    if (probe !== undefined) return probe;
  }
  throw new Error("input series did not produce its systematic sample");
}

function nextSampledCanonical(
  tracker: BrowserPresentationDiagnostics,
  frameKind: "pty-output" | "resize-applied" = "pty-output",
  bytes = 128,
): BrowserCanonicalPresentationProbe {
  for (let attempt = 0; attempt < 64; attempt += 1) {
    const probe = tracker.beginCanonicalIngress(frameKind, bytes);
    if (probe !== undefined) return probe;
  }
  throw new Error("canonical series did not produce its systematic sample");
}

describe("Browser presentation diagnostics", () => {
  it.each([
    [undefined, "memory-v2"],
    [null, "memory-v2"],
    ["", "memory-v2"],
    ["memory-v2", "memory-v2"],
    ["off", "off"],
    ["MEMORY-V2", "off"],
    ["unknown", "off"],
    [1, "off"],
  ] as const)("resolves diagnostics mode %s to %s", (value, expected) => {
    expect(resolveBrowserDiagnosticsMode(value)).toBe(expected);
  });

  it("returns from off mode without touching telemetry, clock, random, or schedulers", () => {
    const touched: PropertyKey[] = [];
    const options = new Proxy(
      {},
      {
        get(_target, property) {
          touched.push(property);
          throw new Error("off mode touched an option");
        },
      },
    ) as BrowserPresentationDiagnosticsOptions;

    expect(createBrowserPresentationDiagnostics("off", options)).toBeUndefined();
    expect(touched).toEqual([]);
  });

  it("uses independent fixed random phases and exact systematic weight 64", () => {
    const harness = createHarness({}, [3, 5]);
    const inputSamples: BrowserInputLifecycleProbe[] = [];
    const canonicalSamples: BrowserCanonicalPresentationProbe[] = [];

    for (let index = 0; index < 128; index += 1) {
      const input = harness.tracker.beginInputDispatch("text");
      if (input !== undefined) {
        inputSamples.push(input);
        harness.tracker.finishInput(input, "not-writable");
      }
      const canonical = harness.tracker.beginCanonicalIngress("pty-output", 9);
      if (canonical !== undefined) {
        canonicalSamples.push(canonical);
        harness.tracker.replicaNotObserved(canonical, "not-live");
      }
    }

    expect(inputSamples).toHaveLength(2);
    expect(canonicalSamples).toHaveLength(2);
    expect(harness.events).toHaveLength(4);
    expect(harness.events.every((event) => event.schemaVersion === 2)).toBe(true);
    expect(
      harness.events.every((event) => "sampleWeight" in event && event.sampleWeight === 64),
    ).toBe(true);
  });

  it("disables only the series whose random phase cannot be read", () => {
    let randomRead = 0;
    const harness = createHarness({
      randomUint32: () => {
        randomRead += 1;
        if (randomRead === 1) throw new Error("input random unavailable");
        return 0;
      },
    });

    expect(harness.tracker.beginInputDispatch("key")).toBeUndefined();
    const canonical = harness.tracker.beginCanonicalIngress("pty-output", 1);
    expect(canonical).toBeDefined();
    expect(randomRead).toBe(2);
  });

  it("records dispatch, send decision, and ACK durations without exporting identity", () => {
    const harness = createHarness();
    harness.setNow(10);
    const probe = nextSampledInput(harness.tracker, "paste");
    harness.tracker.markInputQueued(probe);
    harness.setNow(13);
    harness.tracker.recordInputSendDecision(
      probe,
      { inputEpoch: "private-epoch", clientInputSeq: "99" },
      "sent",
    );
    harness.setNow(21);
    harness.tracker.recordInputAck({
      inputEpoch: "private-epoch",
      clientInputSeq: "99",
      status: "duplicate",
    });

    expect(harness.events).toEqual([
      {
        schemaVersion: 2,
        monotonicAtMs: 21,
        clockKind: "browser-performance",
        sampleWeight: 64,
        name: "browser.input.lifecycle",
        outcome: "ack-received",
        inputKind: "paste",
        status: "duplicate",
        dispatchToSendDecisionMs: 3,
        sendDecisionToAckMs: 8,
        dispatchToAckMs: 11,
        outstandingInputs: 0,
      },
    ]);
    expect(JSON.stringify(harness.events)).not.toContain("private");
    harness.tracker.recordInputAck({
      inputEpoch: "private-epoch",
      clientInputSeq: "99",
      status: "written",
    });
    expect(harness.events).toHaveLength(1);
  });

  it("records local terminal stage and maps explicit send outcomes", () => {
    const harness = createHarness();

    const dispatch = nextSampledInput(harness.tracker, "key");
    harness.tracker.finishInput(dispatch, "not-writable");

    const queued = nextSampledInput(harness.tracker, "mouse");
    harness.tracker.markInputQueued(queued);
    harness.tracker.finishInput(queued, "coalesced");

    const rejected = nextSampledInput(harness.tracker, "text");
    harness.tracker.recordInputSendDecision(
      rejected,
      { inputEpoch: "never-output", clientInputSeq: "1" },
      "rejected",
    );

    const uncertain = nextSampledInput(harness.tracker, "focus");
    harness.tracker.recordInputSendDecision(
      uncertain,
      { inputEpoch: "never-output", clientInputSeq: "2" },
      "uncertain",
    );

    expect(
      harness.events.map((event) =>
        event.name === "browser.input.lifecycle" && event.outcome === "terminal"
          ? [event.reason, event.stage]
          : [],
      ),
    ).toEqual([
      ["not-writable", "dispatch"],
      ["coalesced", "queued"],
      ["send-returned-false", "send-decision"],
      ["send-threw", "send-decision"],
    ]);
  });

  it("ends every sampled input in an epoch exactly once", () => {
    const harness = createHarness();
    const queued = nextSampledInput(harness.tracker);
    harness.tracker.markInputQueued(queued);
    const awaitingAck = nextSampledInput(harness.tracker);
    harness.tracker.recordInputSendDecision(
      awaitingAck,
      { inputEpoch: "epoch", clientInputSeq: "1" },
      "sent",
    );

    harness.tracker.endInputEpoch("transport-replaced");
    harness.tracker.finishInput(queued, "deadline");
    harness.tracker.recordInputAck({
      inputEpoch: "epoch",
      clientInputSeq: "1",
      status: "written",
    });

    expect(harness.events).toHaveLength(2);
    expect(harness.events).toMatchObject([
      { outcome: "terminal", reason: "transport-replaced", stage: "queued" },
      { outcome: "terminal", reason: "transport-replaced", stage: "awaiting-ack" },
    ]);
  });

  it("enforces a shared hard cap and emits sampled capacity outcomes", () => {
    const harness = createHarness();
    for (let index = 0; index < 64; index += 1) {
      nextSampledInput(harness.tracker, "key");
    }

    expect(nextSampledInput.bind(undefined, harness.tracker, "text")).toThrow(
      "input series did not produce",
    );
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        name: "browser.input.lifecycle",
        outcome: "terminal",
        reason: "pending-capacity",
        stage: "dispatch",
      }),
    );

    expect(nextSampledCanonical.bind(undefined, harness.tracker)).toThrow(
      "canonical series did not produce",
    );
    expect(harness.events).toContainEqual(
      expect.objectContaining({
        name: "browser.presentation.canonical",
        outcome: "not-observed",
        reason: "capacity",
        stage: "ingress",
      }),
    );
  });

  it("samples before allocating probes, reading the clock, bucketing bytes, or scheduling", () => {
    const monotonicNow = vi.fn(() => 10);
    const harness = createHarness({ monotonicNow }, [1, 1]);

    expect(harness.tracker.beginInputDispatch("key")).toBeUndefined();
    expect(harness.tracker.beginCanonicalIngress("pty-output", -1)).toBeUndefined();
    expect(monotonicNow).not.toHaveBeenCalled();
    expect(harness.setTimer).not.toHaveBeenCalled();

    expect(harness.tracker.beginInputDispatch("key")).toBeDefined();
    expect(monotonicNow).toHaveBeenCalled();
  });

  it("uses one earliest-deadline timer and expires all due stages", () => {
    const harness = createHarness();
    const first = nextSampledInput(harness.tracker);
    harness.tracker.markInputQueued(first);
    const second = nextSampledCanonical(harness.tracker);
    harness.tracker.replicaApplied(second);

    expect(harness.setTimer).toHaveBeenCalledTimes(1);
    expect([...harness.timers.values()].filter((timer) => !timer.cancelled)).toHaveLength(1);
    harness.setNow(2_000);
    harness.invokeActiveTimer();

    expect(harness.events).toMatchObject([
      { name: "browser.input.lifecycle", reason: "deadline", stage: "queued" },
      { name: "browser.presentation.canonical", reason: "deadline", stage: "applied" },
    ]);
    expect([...harness.timers.values()].filter((timer) => !timer.cancelled)).toHaveLength(0);
  });

  it("enforces the input deadline even when its timer callback is delayed", () => {
    const harness = createHarness();
    const probe = nextSampledInput(harness.tracker);
    harness.tracker.recordInputSendDecision(
      probe,
      { inputEpoch: "epoch", clientInputSeq: "1" },
      "sent",
    );

    harness.setNow(2_001);
    harness.tracker.recordInputAck({
      inputEpoch: "epoch",
      clientInputSeq: "1",
      status: "written",
    });

    expect(harness.events).toEqual([
      expect.objectContaining({
        name: "browser.input.lifecycle",
        outcome: "terminal",
        reason: "deadline",
        stage: "awaiting-ack",
        observedDurationMs: 2_001,
      }),
    ]);
  });

  it("enforces the presentation deadline before a delayed frame opportunity", () => {
    const harness = createHarness();
    const probe = nextSampledCanonical(harness.tracker);
    harness.tracker.replicaApplied(probe);
    harness.tracker.renderCommitted();

    harness.setNow(2_001);
    harness.invokeActiveFrame();

    expect(harness.events).toEqual([
      expect.objectContaining({
        name: "browser.presentation.canonical",
        outcome: "not-observed",
        reason: "deadline",
        stage: "render-committed",
        observedDurationMs: 2_001,
      }),
    ]);
  });

  it("expires an applied probe and cancels its no-longer-needed deadline timer at commit", () => {
    const harness = createHarness();
    const probe = nextSampledCanonical(harness.tracker);
    harness.tracker.replicaApplied(probe);

    harness.setNow(2_001);
    harness.tracker.renderCommitted();

    expect(harness.events).toEqual([
      expect.objectContaining({
        name: "browser.presentation.canonical",
        outcome: "not-observed",
        reason: "deadline",
        stage: "applied",
      }),
    ]);
    expect(harness.requestAnimationFrame).not.toHaveBeenCalled();
    expect([...harness.timers.values()].filter((timer) => !timer.cancelled)).toHaveLength(0);
  });

  it("coalesces all applied probes behind one render commit and one next-frame opportunity", () => {
    const harness = createHarness();
    harness.setNow(1);
    const first = nextSampledCanonical(harness.tracker, "pty-output", 65);
    harness.setNow(2);
    harness.tracker.replicaApplied(first);
    const second = nextSampledCanonical(harness.tracker, "resize-applied", 8);
    harness.setNow(3);
    harness.tracker.replicaApplied(second);
    harness.setNow(5);
    harness.tracker.renderCommitted();
    harness.tracker.renderCommitted();

    expect(harness.requestAnimationFrame).toHaveBeenCalledOnce();
    harness.setNow(9);
    harness.invokeActiveFrame();

    expect(harness.events).toEqual([
      expect.objectContaining({
        name: "browser.presentation.canonical",
        outcome: "next-frame-opportunity",
        frameKind: "pty-output",
        frameBytesBucket: "65-1024",
        ingressToReplicaApplyMs: 1,
        replicaApplyToRenderCommitMs: 3,
        renderCommitToFrameOpportunityMs: 4,
        totalDurationMs: 8,
      }),
      expect.objectContaining({
        name: "browser.presentation.canonical",
        outcome: "next-frame-opportunity",
        frameKind: "resize-applied",
        frameBytesBucket: "1-8",
        ingressToReplicaApplyMs: 1,
        replicaApplyToRenderCommitMs: 2,
        renderCommitToFrameOpportunityMs: 4,
        totalDurationMs: 7,
      }),
    ]);
  });

  it("records page-hidden at ingress, applied, and render-committed stages", () => {
    const stages: string[] = [];
    for (const targetStage of ["ingress", "applied", "render-committed"] as const) {
      const harness = createHarness();
      const probe = nextSampledCanonical(harness.tracker);
      if (targetStage !== "ingress") harness.tracker.replicaApplied(probe);
      if (targetStage === "render-committed") harness.tracker.renderCommitted();
      harness.tracker.setPageVisible(false);
      const event = harness.events[0];
      if (event?.name === "browser.presentation.canonical" && event.outcome === "not-observed") {
        stages.push(event.stage);
        expect(event.reason).toBe("page-hidden");
      }
      if (targetStage === "render-committed") {
        expect(harness.cancelAnimationFrame).toHaveBeenCalledOnce();
      }
    }
    expect(stages).toEqual(["ingress", "applied", "render-committed"]);
  });

  it("keeps sampled canonical ingress terminal while hidden and resumes only when visible", () => {
    const harness = createHarness();
    harness.tracker.setPageVisible(false);

    expect(harness.tracker.beginCanonicalIngress("pty-output", 64)).toBeUndefined();
    expect(harness.events).toMatchObject([
      {
        name: "browser.presentation.canonical",
        outcome: "not-observed",
        reason: "page-hidden",
        stage: "ingress",
      },
    ]);
    expect(nextSampledInput(harness.tracker)).toBeDefined();

    harness.tracker.setPageVisible(true);
    expect(nextSampledCanonical(harness.tracker)).toBeDefined();
  });

  it("fences late frame and timer callbacks after generation end and close", () => {
    const harness = createHarness();
    const canonical = nextSampledCanonical(harness.tracker);
    harness.tracker.replicaApplied(canonical);
    harness.tracker.renderCommitted();
    const staleFrame = [...harness.frames.values()][0];
    harness.tracker.endPresentationGeneration("generation-ended");
    staleFrame?.callback(100);

    const input = nextSampledInput(harness.tracker);
    harness.tracker.markInputQueued(input);
    const staleTimer = [...harness.timers.values()].at(-1);
    harness.tracker.close();
    staleTimer?.callback();
    harness.tracker.close();

    expect(harness.events).toMatchObject([
      {
        name: "browser.presentation.canonical",
        outcome: "not-observed",
        reason: "generation-ended",
        stage: "render-committed",
      },
      {
        name: "browser.input.lifecycle",
        outcome: "terminal",
        reason: "session-closed",
        stage: "queued",
      },
    ]);
    expect(harness.events).toHaveLength(2);
  });

  it("contains clock, telemetry, timer, frame, clear, and cancel failures", () => {
    const badClock = createHarness({
      monotonicNow: () => {
        throw new Error("clock");
      },
    });
    expect(() => badClock.tracker.beginInputDispatch("key")).not.toThrow();
    expect(badClock.tracker.beginInputDispatch("key")).toBeUndefined();

    const badTelemetry = createHarness({
      telemetry: () => {
        throw new Error("sink");
      },
    });
    const rejected = nextSampledInput(badTelemetry.tracker);
    expect(() => badTelemetry.tracker.finishInput(rejected, "not-writable")).not.toThrow();

    const badTimer = createHarness({
      setTimer: () => {
        throw new Error("timer");
      },
    });
    expect(() => nextSampledInput(badTimer.tracker)).not.toThrow();

    const badFrame = createHarness({
      requestAnimationFrame: () => {
        throw new Error("frame");
      },
      clearTimer: () => {
        throw new Error("clear");
      },
      cancelAnimationFrame: () => {
        throw new Error("cancel");
      },
    });
    const canonical = nextSampledCanonical(badFrame.tracker);
    badFrame.tracker.replicaApplied(canonical);
    expect(() => badFrame.tracker.renderCommitted()).not.toThrow();
    expect(() => badFrame.tracker.setPageVisible(false)).not.toThrow();
    expect(() => badFrame.tracker.close()).not.toThrow();
  });

  it("does not retain identities, content, URLs, errors, hashes, or paint claims", () => {
    const harness = createHarness();
    const input = nextSampledInput(harness.tracker, "text");
    harness.tracker.recordInputSendDecision(
      input,
      { inputEpoch: "secret-epoch", clientInputSeq: "secret-sequence" },
      "sent",
    );
    harness.tracker.recordInputAck({
      inputEpoch: "secret-epoch",
      clientInputSeq: "secret-sequence",
      status: "written",
    });
    const canonical = nextSampledCanonical(harness.tracker, "pty-output", 123);
    harness.tracker.replicaNotObserved(canonical, "not-live");

    const serialized = JSON.stringify(harness.events);
    for (const forbidden of [
      "secret",
      "content",
      "url",
      "error",
      "hash",
      "paint",
      "inputEpoch",
      "clientInputSeq",
    ]) {
      expect(serialized).not.toContain(forbidden);
    }
  });
});
