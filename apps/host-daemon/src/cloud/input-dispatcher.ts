import type { HostControlFrame, RelayToHostControlFrame } from "@zhongduan/protocol";

import type { SemanticInputIdentity, SubmittedInputTiming, TerminalSession } from "../session";
import type { SemanticKey, SemanticMouse } from "../terminal-authority";

export type ForwardedInput = Extract<
  RelayToHostControlFrame,
  { type: "key" | "text" | "paste" | "focus" | "mouse" | "resize-request" }
>;

export interface DispatchedInput {
  ack: Extract<HostControlFrame, { type: "input-ack" }>;
  timing: SubmittedInputTiming;
}

export async function dispatchForwardedInput(
  session: TerminalSession,
  frame: ForwardedInput,
): Promise<DispatchedInput> {
  const identity: SemanticInputIdentity = {
    clientId: frame.clientId,
    clientInputSeq: BigInt(frame.clientInputSeq),
    inputEpoch: frame.inputEpoch,
    writerFence: BigInt(frame.writerFence),
    ...(frame.type === "key" ? { observedEventSeq: BigInt(frame.observedEventSeq) } : {}),
  };

  const result = await submit(session, identity, frame);
  return {
    ack: {
      type: "input-ack",
      connectionId: frame.connectionId,
      inputEpoch: result.inputEpoch,
      clientInputSeq: result.clientInputSeq.toString(),
      status: result.status,
      authorityEventSeq: result.authorityEventSeq.toString(),
    },
    timing: result.timing,
  };
}

function submit(session: TerminalSession, identity: SemanticInputIdentity, frame: ForwardedInput) {
  switch (frame.type) {
    case "key": {
      const key: SemanticKey = {
        action: frame.action,
        altGraph: frame.altGraph,
        code: frame.code,
        composing: frame.composing,
        consumedModifiers: frame.consumedModifiers,
        key: frame.key,
        modifiers: frame.modifiers,
        ...(frame.text === undefined ? {} : { text: frame.text }),
        ...(frame.unshiftedCodepoint === undefined
          ? {}
          : { unshiftedCodepoint: frame.unshiftedCodepoint }),
      };
      return session.submitKey(identity, key);
    }
    case "text":
      return session.submitText(identity, frame.data);
    case "paste":
      return session.submitPaste(identity, frame.data);
    case "focus":
      return session.submitFocus(identity, frame.focused);
    case "mouse":
      return session.submitMouse(identity, mouseIntent(frame));
    case "resize-request":
      return session.submitResize(identity, {
        cols: frame.cols,
        rows: frame.rows,
        widthPx: frame.widthPx,
        heightPx: frame.heightPx,
      });
  }
}

function mouseIntent(frame: Extract<ForwardedInput, { type: "mouse" }>): SemanticMouse {
  const base = {
    altGraph: frame.altGraph,
    buttons: frame.buttons,
    modifiers: frame.modifiers,
    surface: { ...frame.surface },
  };
  if (frame.action === "wheel") {
    return {
      ...base,
      action: frame.action,
      button: frame.button,
      deltaMode: frame.deltaMode,
      ...(frame.deltaX === undefined ? {} : { deltaX: frame.deltaX }),
      ...(frame.deltaY === undefined ? {} : { deltaY: frame.deltaY }),
    };
  }
  if (frame.action === "move") return { ...base, action: "move", button: null };
  if (frame.action === "press") return { ...base, action: "press", button: frame.button };
  return { ...base, action: "release", button: frame.button };
}
