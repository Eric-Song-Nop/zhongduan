import {
  telemetryByteSizeBucket,
  type BrowserTelemetrySink,
  type TelemetryByteSizeBucket,
} from "@zhongduan/telemetry";

const SAMPLE_WEIGHT = 64;
const MAX_PENDING = 64;
const DEADLINE_MS = 2_000;

export type BrowserDiagnosticsMode = "off" | "memory-v2";
export type BrowserInputKind = "key" | "text" | "paste" | "focus" | "mouse" | "resize";
export type BrowserInputStatus = "written" | "duplicate" | "rejected" | "uncertain";
export type BrowserInputTerminalReason =
  | "not-writable"
  | "policy-rejected"
  | "validation-failed"
  | "send-returned-false"
  | "send-threw"
  | "coalesced"
  | "pending-capacity"
  | "deadline"
  | "input-epoch-ended"
  | "transport-replaced"
  | "session-closed";
export type BrowserInputEpochEndReason = Extract<
  BrowserInputTerminalReason,
  "input-epoch-ended" | "transport-replaced" | "session-closed"
>;
export type BrowserCanonicalFrameKind = "pty-output" | "resize-applied";
export type BrowserPresentationNotObservedReason =
  | "not-live"
  | "generation-ended"
  | "page-hidden"
  | "deadline"
  | "apply-failed"
  | "session-closed"
  | "capacity";

declare const inputProbeBrand: unique symbol;
declare const canonicalProbeBrand: unique symbol;

/** Opaque sampled input handle. It deliberately carries no protocol identity. */
export interface BrowserInputLifecycleProbe {
  readonly [inputProbeBrand]: never;
}

/** Opaque sampled presentation handle. It deliberately carries no frame content or identity. */
export interface BrowserCanonicalPresentationProbe {
  readonly [canonicalProbeBrand]: never;
}

export interface BrowserInputIdentity {
  readonly inputEpoch: string;
  readonly clientInputSeq: string;
}

export interface BrowserInputAcknowledgement extends BrowserInputIdentity {
  readonly status: BrowserInputStatus;
}

export type BrowserInputSendDecision = "sent" | "rejected" | "uncertain";

export interface BrowserPresentationDiagnosticsOptions {
  telemetry: BrowserTelemetrySink;
  monotonicNow?: () => number;
  randomUint32?: () => number;
  setTimer?: (callback: () => void, delayMs: number) => unknown;
  clearTimer?: (handle: unknown) => void;
  requestAnimationFrame?: (callback: (timestamp: number) => void) => unknown;
  cancelAnimationFrame?: (handle: unknown) => void;
}

export interface BrowserPresentationDiagnostics {
  beginInputDispatch(kind: BrowserInputKind): BrowserInputLifecycleProbe | undefined;
  markInputQueued(probe: BrowserInputLifecycleProbe | undefined): void;
  recordInputSendDecision(
    probe: BrowserInputLifecycleProbe | undefined,
    identity: BrowserInputIdentity,
    decision: BrowserInputSendDecision,
  ): void;
  recordInputAck(acknowledgement: BrowserInputAcknowledgement): void;
  finishInput(
    probe: BrowserInputLifecycleProbe | undefined,
    reason: BrowserInputTerminalReason,
  ): void;
  endInputEpoch(reason: BrowserInputEpochEndReason): void;
  beginCanonicalIngress(
    frameKind: BrowserCanonicalFrameKind,
    bytes: number,
  ): BrowserCanonicalPresentationProbe | undefined;
  replicaApplied(probe: BrowserCanonicalPresentationProbe | undefined): void;
  replicaNotObserved(
    probe: BrowserCanonicalPresentationProbe | undefined,
    reason: BrowserPresentationNotObservedReason,
  ): void;
  renderCommitted(): void;
  endPresentationGeneration(reason: BrowserPresentationNotObservedReason): void;
  setPageVisible(visible: boolean): void;
  close(): void;
}

type InputStage = "dispatch" | "queued" | "send-decision" | "awaiting-ack";
type CanonicalStage = "ingress" | "applied" | "render-committed";

interface InputProbeState {
  readonly category: "input";
  readonly deadlineAt: number;
  readonly inputKind: BrowserInputKind;
  readonly probe: BrowserInputLifecycleProbe;
  readonly startedAt: number;
  identityKey?: string;
  sendDecisionAt?: number;
  stage: InputStage;
}

interface CanonicalProbeState {
  readonly category: "canonical";
  readonly deadlineAt: number;
  readonly frameBytesBucket: TelemetryByteSizeBucket;
  readonly frameKind: BrowserCanonicalFrameKind;
  readonly probe: BrowserCanonicalPresentationProbe;
  readonly startedAt: number;
  appliedAt?: number;
  renderCommittedAt?: number;
  stage: CanonicalStage;
}

type PendingProbeState = InputProbeState | CanonicalProbeState;

function defaultRandomUint32(): number {
  const value = new Uint32Array(1);
  globalThis.crypto.getRandomValues(value);
  return value[0] ?? 0;
}

function inputIdentityKey(identity: BrowserInputIdentity): string {
  return `${identity.inputEpoch}\u0000${identity.clientInputSeq}`;
}

function elapsed(startedAt: number, finishedAt: number): number {
  return Math.max(0, finishedAt - startedAt);
}

function finiteNonnegative(value: number): boolean {
  return Number.isFinite(value) && value >= 0;
}

export function resolveBrowserDiagnosticsMode(value: unknown): BrowserDiagnosticsMode {
  if (value === undefined || value === null || value === "") return "memory-v2";
  return value === "memory-v2" ? "memory-v2" : "off";
}

/**
 * Creates an in-memory, sampled observer. The off branch returns before touching any option,
 * clock, random source, timer, or animation-frame scheduler.
 */
export function createBrowserPresentationDiagnostics(
  mode: BrowserDiagnosticsMode,
  options: BrowserPresentationDiagnosticsOptions,
): BrowserPresentationDiagnostics | undefined {
  if (mode === "off") return undefined;

  const telemetry = options.telemetry;
  const monotonicNow = options.monotonicNow ?? (() => globalThis.performance.now());
  const randomUint32 = options.randomUint32 ?? defaultRandomUint32;
  const setTimer =
    options.setTimer ??
    ((callback: () => void, delay: number) => globalThis.setTimeout(callback, delay));
  const clearTimer =
    options.clearTimer ?? ((handle: unknown) => globalThis.clearTimeout(handle as number));
  const requestFrame =
    options.requestAnimationFrame ??
    ((callback: (timestamp: number) => void) => globalThis.requestAnimationFrame(callback));
  const cancelFrame =
    options.cancelAnimationFrame ??
    ((handle: unknown) => globalThis.cancelAnimationFrame(handle as number));

  const readSamplingPhase = (): number | undefined => {
    try {
      const value = randomUint32();
      return Number.isSafeInteger(value) && value >= 0 && value <= 0xffff_ffff
        ? value % SAMPLE_WEIGHT
        : undefined;
    } catch {
      return undefined;
    }
  };
  const inputSamplingPhase = readSamplingPhase();
  const canonicalSamplingPhase = readSamplingPhase();
  let inputSampleCursor = 0;
  let canonicalSampleCursor = 0;
  let closed = false;
  let pageVisible = true;

  const pending = new Set<PendingProbeState>();
  const inputByProbe = new Map<BrowserInputLifecycleProbe, InputProbeState>();
  const canonicalByProbe = new Map<BrowserCanonicalPresentationProbe, CanonicalProbeState>();
  const inputByIdentity = new Map<string, InputProbeState>();

  let deadlineTimerScheduled = false;
  let deadlineTimerHandle: unknown;
  let deadlineTimerDueAt: number | undefined;
  let deadlineTimerToken = 0;
  let frameScheduled = false;
  let frameHandle: unknown;
  let frameToken = 0;

  const shouldSampleInput = () => {
    if (inputSamplingPhase === undefined) return false;
    const selected = inputSampleCursor === inputSamplingPhase;
    inputSampleCursor = (inputSampleCursor + 1) % SAMPLE_WEIGHT;
    return selected;
  };
  const shouldSampleCanonical = () => {
    if (canonicalSamplingPhase === undefined) return false;
    const selected = canonicalSampleCursor === canonicalSamplingPhase;
    canonicalSampleCursor = (canonicalSampleCursor + 1) % SAMPLE_WEIGHT;
    return selected;
  };
  const readNow = (): number | undefined => {
    try {
      const value = monotonicNow();
      return finiteNonnegative(value) ? value : undefined;
    } catch {
      return undefined;
    }
  };
  const observedAt = (fallback: number): number => {
    const value = readNow();
    return value === undefined ? fallback : Math.max(fallback, value);
  };
  const emit = (event: Parameters<BrowserTelemetrySink>[0]) => {
    try {
      telemetry(event);
    } catch {
      // Diagnostics must never alter terminal input, recovery, or rendering.
    }
  };

  const cancelDeadlineTimer = () => {
    deadlineTimerToken += 1;
    if (!deadlineTimerScheduled) return;
    deadlineTimerScheduled = false;
    deadlineTimerDueAt = undefined;
    try {
      clearTimer(deadlineTimerHandle);
    } catch {
      // A stale callback is fenced by deadlineTimerToken.
    }
  };

  const cancelPresentationFrame = () => {
    frameToken += 1;
    if (!frameScheduled) return;
    frameScheduled = false;
    try {
      cancelFrame(frameHandle);
    } catch {
      // A stale callback is fenced by frameToken.
    }
  };

  const removeInputState = (state: InputProbeState) => {
    pending.delete(state);
    inputByProbe.delete(state.probe);
    if (state.identityKey !== undefined && inputByIdentity.get(state.identityKey) === state) {
      inputByIdentity.delete(state.identityKey);
    }
  };
  const removeCanonicalState = (state: CanonicalProbeState) => {
    pending.delete(state);
    canonicalByProbe.delete(state.probe);
  };

  const emitInputTerminal = (
    state: InputProbeState,
    reason: BrowserInputTerminalReason,
    finishedAt: number,
  ) => {
    removeInputState(state);
    emit({
      schemaVersion: 2,
      monotonicAtMs: finishedAt,
      clockKind: "browser-performance",
      sampleWeight: SAMPLE_WEIGHT,
      name: "browser.input.lifecycle",
      outcome: "terminal",
      inputKind: state.inputKind,
      reason,
      stage: state.stage,
      observedDurationMs: elapsed(state.startedAt, finishedAt),
    });
  };
  const emitCanonicalTerminal = (
    state: CanonicalProbeState,
    reason: BrowserPresentationNotObservedReason,
    finishedAt: number,
  ) => {
    removeCanonicalState(state);
    emit({
      schemaVersion: 2,
      monotonicAtMs: finishedAt,
      clockKind: "browser-performance",
      sampleWeight: SAMPLE_WEIGHT,
      name: "browser.presentation.canonical",
      outcome: "not-observed",
      frameKind: state.frameKind,
      frameBytesBucket: state.frameBytesBucket,
      reason,
      stage: state.stage,
      observedDurationMs: elapsed(state.startedAt, finishedAt),
    });
  };
  const expireInputIfDue = (state: InputProbeState, observedAt: number): boolean => {
    if (observedAt < state.deadlineAt) return false;
    emitInputTerminal(state, "deadline", observedAt);
    return true;
  };
  const expireCanonicalIfDue = (state: CanonicalProbeState, observedAt: number): boolean => {
    if (observedAt < state.deadlineAt) return false;
    emitCanonicalTerminal(state, "deadline", observedAt);
    return true;
  };

  const hasRenderCommittedProbe = () => {
    for (const state of canonicalByProbe.values()) {
      if (state.stage === "render-committed") return true;
    }
    return false;
  };
  const cancelFrameIfUnused = () => {
    if (frameScheduled && !hasRenderCommittedProbe()) cancelPresentationFrame();
  };

  let scheduleDeadlineTimer = () => undefined;
  const expireDeadlines = (now: number) => {
    for (const state of pending) {
      if (state.deadlineAt > now) continue;
      if (state.category === "input") emitInputTerminal(state, "deadline", now);
      else emitCanonicalTerminal(state, "deadline", now);
    }
    cancelFrameIfUnused();
  };

  const failClosedAfterTimerError = () => {
    for (const state of pending) {
      const finishedAt = Math.max(state.startedAt, readNow() ?? state.startedAt);
      if (state.category === "input") emitInputTerminal(state, "deadline", finishedAt);
      else emitCanonicalTerminal(state, "deadline", finishedAt);
    }
    cancelFrameIfUnused();
  };

  scheduleDeadlineTimer = () => {
    let earliest: number | undefined;
    for (const state of pending) {
      if (earliest === undefined || state.deadlineAt < earliest) earliest = state.deadlineAt;
    }
    if (earliest === undefined) {
      cancelDeadlineTimer();
      return;
    }
    if (deadlineTimerScheduled && deadlineTimerDueAt === earliest) return;
    cancelDeadlineTimer();
    const now = readNow();
    const delay = now === undefined ? DEADLINE_MS : Math.max(0, earliest - now);
    const token = ++deadlineTimerToken;
    deadlineTimerScheduled = true;
    deadlineTimerDueAt = earliest;
    try {
      const handle = setTimer(() => {
        if (!deadlineTimerScheduled || token !== deadlineTimerToken) return;
        deadlineTimerScheduled = false;
        deadlineTimerDueAt = undefined;
        const current = readNow();
        if (current === undefined) {
          failClosedAfterTimerError();
          scheduleDeadlineTimer();
          return;
        }
        expireDeadlines(current);
        scheduleDeadlineTimer();
      }, delay);
      deadlineTimerHandle = handle;
    } catch {
      deadlineTimerScheduled = false;
      deadlineTimerDueAt = undefined;
      failClosedAfterTimerError();
    }
  };

  const terminateAllInputs = (reason: BrowserInputEpochEndReason, reschedule = true) => {
    const now = readNow();
    for (const state of inputByProbe.values()) {
      const finishedAt = Math.max(state.startedAt, now ?? state.startedAt);
      if (!expireInputIfDue(state, finishedAt)) {
        emitInputTerminal(state, reason, finishedAt);
      }
    }
    if (reschedule) scheduleDeadlineTimer();
  };
  const terminateAllCanonical = (
    reason: BrowserPresentationNotObservedReason,
    reschedule = true,
  ) => {
    const now = readNow();
    for (const state of canonicalByProbe.values()) {
      const finishedAt = Math.max(state.startedAt, now ?? state.startedAt);
      if (!expireCanonicalIfDue(state, finishedAt)) {
        emitCanonicalTerminal(state, reason, finishedAt);
      }
    }
    cancelFrameIfUnused();
    if (reschedule) scheduleDeadlineTimer();
  };

  const diagnostics: BrowserPresentationDiagnostics = {
    beginInputDispatch(kind) {
      if (closed || !shouldSampleInput()) return undefined;
      const now = readNow();
      if (now === undefined) return undefined;
      if (pending.size >= MAX_PENDING) {
        emit({
          schemaVersion: 2,
          monotonicAtMs: now,
          clockKind: "browser-performance",
          sampleWeight: SAMPLE_WEIGHT,
          name: "browser.input.lifecycle",
          outcome: "terminal",
          inputKind: kind,
          reason: "pending-capacity",
          stage: "dispatch",
          observedDurationMs: 0,
        });
        return undefined;
      }
      const probe = Object.freeze({}) as BrowserInputLifecycleProbe;
      const state: InputProbeState = {
        category: "input",
        deadlineAt: now + DEADLINE_MS,
        inputKind: kind,
        probe,
        stage: "dispatch",
        startedAt: now,
      };
      pending.add(state);
      inputByProbe.set(probe, state);
      scheduleDeadlineTimer();
      return probe;
    },
    markInputQueued(probe) {
      if (probe === undefined || closed) return;
      const state = inputByProbe.get(probe);
      if (state?.stage !== "dispatch") return;
      const now = observedAt(state.startedAt);
      if (expireInputIfDue(state, now)) {
        scheduleDeadlineTimer();
        return;
      }
      state.stage = "queued";
    },
    recordInputSendDecision(probe, identity, decision) {
      if (probe === undefined || closed) return;
      const state = inputByProbe.get(probe);
      if (state === undefined || state.stage === "awaiting-ack") return;
      const now = observedAt(state.startedAt);
      if (expireInputIfDue(state, now)) {
        scheduleDeadlineTimer();
        return;
      }
      state.stage = "send-decision";
      state.sendDecisionAt = now;
      if (decision !== "sent") {
        emitInputTerminal(
          state,
          decision === "rejected" ? "send-returned-false" : "send-threw",
          now,
        );
        scheduleDeadlineTimer();
        return;
      }
      const key = inputIdentityKey(identity);
      const replaced = inputByIdentity.get(key);
      if (replaced !== undefined && replaced !== state) {
        if (!expireInputIfDue(replaced, now)) {
          emitInputTerminal(replaced, "transport-replaced", now);
        }
      }
      state.stage = "awaiting-ack";
      state.identityKey = key;
      inputByIdentity.set(key, state);
      scheduleDeadlineTimer();
    },
    recordInputAck(acknowledgement) {
      if (closed) return;
      const state = inputByIdentity.get(inputIdentityKey(acknowledgement));
      if (state === undefined || state.sendDecisionAt === undefined) return;
      const now = observedAt(state.sendDecisionAt);
      if (expireInputIfDue(state, now)) {
        scheduleDeadlineTimer();
        return;
      }
      const sendDecisionAt = state.sendDecisionAt;
      removeInputState(state);
      emit({
        schemaVersion: 2,
        monotonicAtMs: now,
        clockKind: "browser-performance",
        sampleWeight: SAMPLE_WEIGHT,
        name: "browser.input.lifecycle",
        outcome: "ack-received",
        inputKind: state.inputKind,
        status: acknowledgement.status,
        dispatchToSendDecisionMs: elapsed(state.startedAt, sendDecisionAt),
        sendDecisionToAckMs: elapsed(sendDecisionAt, now),
        dispatchToAckMs: elapsed(state.startedAt, now),
        outstandingInputs: inputByIdentity.size,
      });
      scheduleDeadlineTimer();
    },
    finishInput(probe, reason) {
      if (probe === undefined || closed) return;
      const state = inputByProbe.get(probe);
      if (state === undefined) return;
      const now = observedAt(state.sendDecisionAt ?? state.startedAt);
      if (!expireInputIfDue(state, now)) emitInputTerminal(state, reason, now);
      scheduleDeadlineTimer();
    },
    endInputEpoch(reason) {
      if (closed) return;
      terminateAllInputs(reason);
    },
    beginCanonicalIngress(frameKind, bytes) {
      if (closed || !shouldSampleCanonical()) return undefined;
      const now = readNow();
      if (now === undefined) return undefined;
      let frameBytesBucket: TelemetryByteSizeBucket;
      try {
        frameBytesBucket = telemetryByteSizeBucket(bytes);
      } catch {
        return undefined;
      }
      if (!pageVisible) {
        emit({
          schemaVersion: 2,
          monotonicAtMs: now,
          clockKind: "browser-performance",
          sampleWeight: SAMPLE_WEIGHT,
          name: "browser.presentation.canonical",
          outcome: "not-observed",
          frameKind,
          frameBytesBucket,
          reason: "page-hidden",
          stage: "ingress",
          observedDurationMs: 0,
        });
        return undefined;
      }
      if (pending.size >= MAX_PENDING) {
        emit({
          schemaVersion: 2,
          monotonicAtMs: now,
          clockKind: "browser-performance",
          sampleWeight: SAMPLE_WEIGHT,
          name: "browser.presentation.canonical",
          outcome: "not-observed",
          frameKind,
          frameBytesBucket,
          reason: "capacity",
          stage: "ingress",
          observedDurationMs: 0,
        });
        return undefined;
      }
      const probe = Object.freeze({}) as BrowserCanonicalPresentationProbe;
      const state: CanonicalProbeState = {
        category: "canonical",
        deadlineAt: now + DEADLINE_MS,
        frameBytesBucket,
        frameKind,
        probe,
        stage: "ingress",
        startedAt: now,
      };
      pending.add(state);
      canonicalByProbe.set(probe, state);
      scheduleDeadlineTimer();
      return probe;
    },
    replicaApplied(probe) {
      if (probe === undefined || closed) return;
      const state = canonicalByProbe.get(probe);
      if (state === undefined || state.stage !== "ingress") return;
      const now = observedAt(state.startedAt);
      if (expireCanonicalIfDue(state, now)) {
        scheduleDeadlineTimer();
        return;
      }
      state.appliedAt = now;
      state.stage = "applied";
    },
    replicaNotObserved(probe, reason) {
      if (probe === undefined || closed) return;
      const state = canonicalByProbe.get(probe);
      if (state === undefined) return;
      const now = observedAt(state.renderCommittedAt ?? state.appliedAt ?? state.startedAt);
      if (!expireCanonicalIfDue(state, now)) emitCanonicalTerminal(state, reason, now);
      cancelFrameIfUnused();
      scheduleDeadlineTimer();
    },
    renderCommitted() {
      if (closed) return;
      const now = readNow();
      let committedAny = false;
      let expiredAny = false;
      for (const state of canonicalByProbe.values()) {
        if (state.stage !== "applied") continue;
        const appliedAt = state.appliedAt ?? state.startedAt;
        const committedAt = Math.max(appliedAt, now ?? appliedAt);
        if (expireCanonicalIfDue(state, committedAt)) {
          expiredAny = true;
          continue;
        }
        state.renderCommittedAt = committedAt;
        state.stage = "render-committed";
        committedAny = true;
      }
      if (expiredAny) scheduleDeadlineTimer();
      if (!committedAny || frameScheduled) return;
      const token = ++frameToken;
      frameScheduled = true;
      try {
        const handle = requestFrame((timestamp) => {
          if (!frameScheduled || token !== frameToken || closed) return;
          frameScheduled = false;
          const clockAt = readNow();
          const validTimestamp = finiteNonnegative(timestamp) ? timestamp : undefined;
          for (const state of canonicalByProbe.values()) {
            if (state.stage !== "render-committed" || state.renderCommittedAt === undefined)
              continue;
            const appliedAt = state.appliedAt ?? state.startedAt;
            const finishedAt = Math.max(
              state.renderCommittedAt,
              clockAt ?? validTimestamp ?? state.renderCommittedAt,
            );
            if (expireCanonicalIfDue(state, finishedAt)) continue;
            removeCanonicalState(state);
            emit({
              schemaVersion: 2,
              monotonicAtMs: finishedAt,
              clockKind: "browser-performance",
              sampleWeight: SAMPLE_WEIGHT,
              name: "browser.presentation.canonical",
              outcome: "next-frame-opportunity",
              frameKind: state.frameKind,
              frameBytesBucket: state.frameBytesBucket,
              ingressToReplicaApplyMs: elapsed(state.startedAt, appliedAt),
              replicaApplyToRenderCommitMs: elapsed(appliedAt, state.renderCommittedAt),
              renderCommitToFrameOpportunityMs: elapsed(state.renderCommittedAt, finishedAt),
              totalDurationMs: elapsed(state.startedAt, finishedAt),
            });
          }
          scheduleDeadlineTimer();
        });
        frameHandle = handle;
      } catch {
        frameScheduled = false;
      }
    },
    endPresentationGeneration(reason) {
      if (closed) return;
      terminateAllCanonical(reason);
    },
    setPageVisible(visible) {
      if (closed) return;
      if (pageVisible === visible) return;
      pageVisible = visible;
      if (!visible) terminateAllCanonical("page-hidden");
    },
    close() {
      if (closed) return;
      closed = true;
      terminateAllInputs("session-closed", false);
      terminateAllCanonical("session-closed", false);
      cancelDeadlineTimer();
      cancelPresentationFrame();
    },
  };

  return diagnostics;
}
