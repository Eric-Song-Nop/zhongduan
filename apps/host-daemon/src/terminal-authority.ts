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

export interface TerminalAuthority {
  readonly engineId: string;

  applyOutput(data: Uint8Array): readonly Uint8Array[];
  resize(dimensions: ResizePayload): readonly Uint8Array[];
  encodeSnapshot(): Uint8Array;
  encodeKey(key: SemanticKey): Uint8Array;
  encodePaste(data: string): Uint8Array;
  dispose(): void;
}
