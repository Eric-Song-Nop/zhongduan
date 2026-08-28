import type { ResizePayload } from "@zhongduan/protocol";

export const KeyModifier = {
  Shift: 1 << 0,
  Alt: 1 << 1,
  Control: 1 << 2,
  Meta: 1 << 3,
} as const;

export interface SemanticKey {
  code: string;
  key: string;
  text?: string;
  modifiers: number;
  repeat: boolean;
}

export interface TerminalAuthority {
  readonly engineId: string;

  applyOutput(data: Uint8Array): readonly Uint8Array[];
  resize(dimensions: ResizePayload): void;
  encodeSnapshot(): Uint8Array;
  encodeKey(key: SemanticKey): Uint8Array;
  encodePaste(data: string): Uint8Array;
  dispose(): void;
}
