import type { TerminalInputEvent, TerminalMouseInputEvent } from "@wterm/core";
import { ClientControlFrameSchema, type ClientControlFrame } from "@zhongduan/protocol";

export type InputFrame = Exclude<
  ClientControlFrame,
  { type: "ack" | "attach" | "writer-lease-renew" }
>;
export type InputKind = TerminalInputEvent["type"] | "unknown";
export type InputPayload = { type: InputFrame["type"] } & Record<string, unknown>;

const textEncoder = new TextEncoder();

function boundedInteger(value: number, maximum = 1_000_000): number {
  if (!Number.isFinite(value)) return 0;
  return Math.min(maximum, Math.max(0, Math.round(value)));
}

function deltaModeName(value: number | undefined): "pixel" | "line" | "page" {
  if (value === 1) return "line";
  if (value === 2) return "page";
  return "pixel";
}

function mousePayload(event: TerminalMouseInputEvent): InputPayload {
  const common = {
    type: "mouse" as const,
    action: event.action,
    button: event.action === "press" || event.action === "release" ? event.button : null,
    buttons: event.buttons,
    modifiers: event.modifiers,
    altGraph: event.altGraph,
    surface: {
      x: boundedInteger(event.surface.x),
      y: boundedInteger(event.surface.y),
    },
  };
  return event.action === "wheel"
    ? {
        ...common,
        deltaX: event.deltaX,
        deltaY: event.deltaY,
        deltaMode: deltaModeName(event.deltaMode),
      }
    : common;
}

export function normalizeInputEvent(
  event: TerminalInputEvent,
  observedEventSeq: bigint,
): { readonly kind: InputKind; readonly payload: InputPayload } {
  let payload: InputPayload;
  switch (event.type) {
    case "key":
      payload = {
        type: "key",
        observedEventSeq: observedEventSeq.toString(),
        code: event.code,
        key: event.key,
        ...(event.text === undefined ? {} : { text: event.text }),
        modifiers: event.modifiers,
        action: event.action,
        altGraph: event.altGraph,
        composing: event.composing,
        consumedModifiers: event.consumedModifiers,
        ...(event.unshiftedCodepoint === 0 ? {} : { unshiftedCodepoint: event.unshiftedCodepoint }),
      };
      break;
    case "text":
      payload = { type: "text", data: event.text };
      break;
    case "paste":
      payload = { type: "paste", data: event.text };
      break;
    case "focus":
      payload = { type: "focus", focused: event.focused };
      break;
    case "resize":
      payload = {
        type: "resize-request",
        cols: event.cols,
        rows: event.rows,
        widthPx: event.widthPx,
        heightPx: event.heightPx,
      };
      break;
    case "mouse":
      payload = mousePayload(event);
      break;
    default:
      throw new Error("unknown input event");
  }
  return {
    kind: payload.type === "resize-request" ? "resize" : payload.type,
    payload,
  };
}

export function isValidInputPayload(payload: InputPayload): boolean {
  return ClientControlFrameSchema.safeParse({
    ...payload,
    writerLease: "validation",
    inputEpoch: "validation",
    clientInputSeq: "1",
  }).success;
}

export function encodedJsonBytes(value: unknown): number {
  return textEncoder.encode(JSON.stringify(value)).byteLength;
}

export function encodeInputFrame(
  payload: InputPayload,
  writerLease: string,
  identity: { readonly inputEpoch: string; readonly clientInputSeq: string },
): { readonly frame: InputFrame; readonly encodedBytes: number } {
  const frame = ClientControlFrameSchema.parse({
    ...payload,
    writerLease,
    inputEpoch: identity.inputEpoch,
    clientInputSeq: identity.clientInputSeq,
  }) as InputFrame;
  return {
    frame: Object.freeze(frame),
    encodedBytes: encodedJsonBytes(frame),
  };
}

export function isCoalesciblePayloadPair(
  left: InputPayload | undefined,
  right: InputPayload,
): boolean {
  if (left === undefined) return false;
  if (left.type === "resize-request" && right.type === "resize-request") return true;
  return (
    left.type === "mouse" &&
    left["action"] === "move" &&
    right.type === "mouse" &&
    right["action"] === "move"
  );
}
