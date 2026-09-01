(() => {
  const state = {
    pageId: crypto.randomUUID(),
    events: [],
    currentSample: null,
    duplicateSample: null,
    nextLocalIntent: 1,
    intents: [],
    productResults: [],
    intentsBySample: Object.create(null),
    pendingByBrowserIdentity: Object.create(null),
    installedSockets: new WeakSet(),
  };
  const now = () => Math.round((performance.timeOrigin + performance.now()) * 1_000_000);
  const record = (name, sampleId) => state.events.push({ name, sampleId, atUnixNs: now() });
  const consume = (sampleId) => {
    if (state.intentsBySample[sampleId] !== undefined) {
      throw new Error(`duplicate E0 UI consumption for ${sampleId}`);
    }
    const intent = {
      sampleId,
      localIntentId: `e0-${state.pageId}-${state.nextLocalIntent++}`,
      consumedAtUnixNs: now(),
      sends: [],
      terminal: null,
    };
    state.intents.push(intent);
    state.intentsBySample[sampleId] = intent;
    return intent.localIntentId;
  };
  const browserIdentity = (frame) =>
    typeof frame?.inputEpoch === "string" && typeof frame?.clientInputSeq === "string"
      ? `${frame.inputEpoch}/${frame.clientInputSeq}`
      : null;
  const sampleFromFrame = (frame) => {
    const identity = browserIdentity(frame);
    if (identity !== null && state.pendingByBrowserIdentity[identity] !== undefined) {
      return state.pendingByBrowserIdentity[identity].sampleId;
    }
    const payload = frame?.type === "paste" || frame?.type === "text" ? frame.data : frame?.text;
    if (typeof payload === "string") {
      for (const pattern of [
        /ZHONGDUAN_E0_PROBE:([A-Za-z0-9_-]+)\r/u,
        /ZHONGDUAN_E0_SECURE:([A-Za-z0-9_-]+)\r/u,
        /ZHONGDUAN_E0_FLOOD:([A-Za-z0-9_-]+)\r/u,
        /ZHONGDUAN_E0_INTERRUPT_ARM:[A-Za-z0-9_-]+:([A-Za-z0-9_-]+)\r/u,
      ]) {
        const match = pattern.exec(payload);
        if (match !== null) return match[1];
      }
    }
    // Paste/text samples carry their marker in-band. Only key events (for example Ctrl-C)
    // need the short-lived UI attribution window. Focus, resize, and mouse reconciliation may
    // legitimately race that window and are independent semantic intents.
    return frame?.type === "key" ? state.currentSample : null;
  };
  const finish = (
    intent,
    outcome,
    reason,
    identity,
    deterministicResult = null,
    source = "passive-wire-observation",
    productLocalIntentId = null,
  ) => {
    if (intent.terminal !== null) return;
    intent.terminal = {
      sampleId: intent.sampleId,
      localIntentId: intent.localIntentId,
      outcome,
      identity,
      reason,
      deterministicResult,
      source,
      productLocalIntentId,
      observedAtUnixNs: now(),
    };
  };
  const acceptAck = (frame) => {
    const identity = browserIdentity(frame);
    if (identity === null) return;
    const intent = state.pendingByBrowserIdentity[identity];
    if (intent === undefined) return;
    const fullIdentity =
      typeof frame.writerFence === "string"
        ? {
            writerFence: frame.writerFence,
            inputEpoch: frame.inputEpoch,
            clientInputSeq: frame.clientInputSeq,
          }
        : null;
    if (frame.status === "uncertain") {
      finish(intent, "uncertain", "input-ack-uncertain", fullIdentity);
    } else {
      finish(intent, "deterministic", "input-ack", fullIdentity, frame.status);
    }
  };
  const installSocketObservers = (socket) => {
    if (state.installedSockets.has(socket)) return;
    state.installedSockets.add(socket);
    socket.addEventListener("message", (event) => {
      if (typeof event.data !== "string" || !event.data.startsWith("{")) return;
      let frame;
      try {
        frame = JSON.parse(event.data);
      } catch {
        return;
      }
      if (frame.type === "input-ack") acceptAck(frame);
      if (frame.type === "input-epoch-ack" && Array.isArray(frame.results)) {
        for (const result of frame.results) acceptAck(result);
      }
    });
    socket.addEventListener("close", () => {
      for (const intent of state.intents) {
        if (intent.terminal === null && intent.sends.some((send) => send.socket === socket)) {
          const last = intent.sends.at(-1);
          finish(intent, "uncertain", "socket-closed-after-send", last?.identity ?? null);
        }
      }
    });
  };
  window.addEventListener("zhongduan:input-intent-result", (event) => {
    const result = event.detail;
    if (result === null || typeof result !== "object") return;
    state.productResults.push({ ...result, observedAtUnixNs: now() });
    const identity = browserIdentity(result.identity);
    const intent =
      identity === null
        ? state.intentsBySample[state.currentSample]
        : state.pendingByBrowserIdentity[identity];
    if (intent === undefined) return;
    finish(
      intent,
      result.outcome,
      result.reason,
      result.identity === null ? null : { ...result.identity },
      result.outcome === "deterministic" ? result.result : null,
      "product-intent-result-event",
      result.localIntentId ?? null,
    );
  });
  // oxlint-disable-next-line typescript/unbound-method -- calls below bind the observed socket.
  const originalSend = WebSocket.prototype.send;
  WebSocket.prototype.send = function (data) {
    let frame = null;
    if (typeof data === "string" && data.startsWith("{")) {
      try {
        frame = JSON.parse(data);
      } catch {}
    }
    const semantic =
      frame !== null &&
      ["key", "text", "paste", "focus", "mouse", "resize-request"].includes(frame.type);
    const sampleId = semantic ? sampleFromFrame(frame) : null;
    if (sampleId !== null) {
      const intent = state.intentsBySample[sampleId];
      const identity = browserIdentity(frame);
      if (intent !== undefined && identity !== null) {
        installSocketObservers(this);
        intent.sends.push({ identity, socket: this, observedAtUnixNs: now() });
        state.pendingByBrowserIdentity[identity] = intent;
        if (intent.sends.length === 1) {
          setTimeout(() => {
            if (intent.terminal === null)
              finish(intent, "uncertain", "observation-deadline-after-send", null);
          }, 35_000);
        }
      }
      record("browser.send-decision", sampleId);
    }
    originalSend.call(this, data);
    if (sampleId !== null && state.duplicateSample === sampleId) {
      state.duplicateSample = null;
      const intent = state.intentsBySample[sampleId];
      const identity = browserIdentity(frame);
      if (intent !== undefined && identity !== null) {
        intent.sends.push({ identity, socket: this, observedAtUnixNs: now() });
      }
      originalSend.call(this, data);
    }
  };
  document.addEventListener(
    "keydown",
    (event) => {
      if (
        event.isTrusted &&
        event.code === "KeyC" &&
        event.key === "c" &&
        event.ctrlKey &&
        !event.altKey &&
        !event.metaKey &&
        !event.shiftKey &&
        state.currentSample !== null
      ) {
        consume(state.currentSample);
        record("browser.keydown", state.currentSample);
        record("browser.ctrl-c", state.currentSample);
        record("browser.input-consumed", state.currentSample);
      }
    },
    true,
  );
  Object.defineProperty(window, "__zhongduanE0", {
    configurable: false,
    value: Object.assign(state, { consume }),
  });
})();
