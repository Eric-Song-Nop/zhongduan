import type { ResizePayload } from "@zhongduan/protocol";

export interface SemanticKey {
  action: "press" | "repeat" | "release";
  altGraph: boolean;
  code: string;
  composing: boolean;
  consumedModifiers: number;
  key: string;
  text?: string;
  modifiers: number;
  unshiftedCodepoint?: number;
}

interface SemanticMouseBase {
  altGraph: boolean;
  buttons: number;
  modifiers: number;
  surface: { x: number; y: number };
}

export type SemanticMouse =
  | (SemanticMouseBase & {
      action: "press" | "release";
      button: number;
    })
  | (SemanticMouseBase & {
      action: "move";
      button: null;
    })
  | (SemanticMouseBase & {
      action: "wheel";
      button: null;
      deltaMode: "pixel" | "line" | "page";
      deltaX?: number;
      deltaY?: number;
    });

export function validateSemanticMouse(
  mouse: SemanticMouse,
  geometry: Pick<ResizePayload, "widthPx" | "heightPx">,
): void {
  if (geometry.widthPx === 0 || geometry.heightPx === 0) {
    throw new RangeError("mouse input requires authoritative pixel geometry");
  }
  if (
    typeof mouse.altGraph !== "boolean" ||
    !Number.isInteger(mouse.surface.x) ||
    mouse.surface.x < 0 ||
    mouse.surface.x > 1_000_000 ||
    !Number.isInteger(mouse.surface.y) ||
    mouse.surface.y < 0 ||
    mouse.surface.y > 1_000_000 ||
    !Number.isInteger(mouse.buttons) ||
    mouse.buttons < 0 ||
    mouse.buttons > 31 ||
    !Number.isInteger(mouse.modifiers) ||
    mouse.modifiers < 0 ||
    mouse.modifiers > 0x3f
  ) {
    throw new RangeError("invalid mouse input");
  }

  const dynamic = mouse as SemanticMouse & {
    deltaMode?: unknown;
    deltaX?: unknown;
    deltaY?: unknown;
  };
  if (mouse.action === "press" || mouse.action === "release") {
    if (!Number.isInteger(mouse.button) || mouse.button < 0 || mouse.button > 4) {
      throw new RangeError("invalid mouse button");
    }
  } else if (mouse.action === "move") {
    if (mouse.button !== null) throw new RangeError("mouse move requires a null button");
  } else if (mouse.action === "wheel") {
    const deltaX = mouse.deltaX ?? 0;
    const deltaY = mouse.deltaY ?? 0;
    if (
      mouse.button !== null ||
      (mouse.deltaMode !== "pixel" && mouse.deltaMode !== "line" && mouse.deltaMode !== "page") ||
      !Number.isFinite(deltaX) ||
      !Number.isFinite(deltaY) ||
      Math.abs(deltaX) > 1_000_000 ||
      Math.abs(deltaY) > 1_000_000 ||
      (deltaX === 0 && deltaY === 0)
    ) {
      throw new RangeError("invalid mouse wheel input");
    }
    return;
  } else {
    throw new RangeError("invalid mouse action");
  }

  if (
    dynamic.deltaMode !== undefined ||
    dynamic.deltaX !== undefined ||
    dynamic.deltaY !== undefined
  ) {
    throw new RangeError("non-wheel mouse input cannot include wheel fields");
  }
}

export interface TerminalAuthority {
  readonly engineId: string;

  applyOutput(data: Uint8Array): readonly Uint8Array[];
  resize(dimensions: ResizePayload): readonly Uint8Array[];
  encodeSnapshot(): Uint8Array;
  encodeKey(key: SemanticKey): Uint8Array;
  encodePaste(data: string): Uint8Array;
  encodeFocus(focused: boolean): Uint8Array;
  validateMouse(mouse: SemanticMouse): void;
  encodeMouse(mouse: SemanticMouse): Uint8Array;
  dispose(): void;
}
