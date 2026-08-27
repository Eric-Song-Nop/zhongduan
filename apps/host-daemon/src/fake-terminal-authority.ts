import type { ResizePayload } from "@zhongduan/protocol";

import { KeyModifier, type SemanticKey, type TerminalAuthority } from "./terminal-authority";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
export const MAX_PASTE_BYTES = 1024 * 1024;
const DEFAULT_MAX_OPERATION_BYTES = 1024 * 1024;

export type FakeAuthorityOperation =
  | { type: "output"; data: Uint8Array }
  | { type: "resize"; dimensions: ResizePayload };

export class FakeTerminalAuthority implements TerminalAuthority {
  readonly engineId = "fake-terminal-authority/v1";
  readonly operations: FakeAuthorityOperation[] = [];
  readonly #maxOperationBytes: number;
  #applicationCursorKeys = false;
  #controlTail = "";
  #operationBytes = 0;

  constructor(options: { maxOperationBytes?: number } = {}) {
    this.#maxOperationBytes = options.maxOperationBytes ?? DEFAULT_MAX_OPERATION_BYTES;
    if (!Number.isInteger(this.#maxOperationBytes) || this.#maxOperationBytes <= 0) {
      throw new RangeError("maxOperationBytes must be a positive integer");
    }
  }

  applyOutput(data: Uint8Array): readonly Uint8Array[] {
    this.#record({ type: "output", data: data.slice() });
    this.#controlTail = `${this.#controlTail}${textDecoder.decode(data, { stream: true })}`;
    const hasStatusQuery = this.#controlTail.includes("\u001b[6n");
    this.#controlTail = this.#controlTail.replaceAll("\u001b[6n", "").slice(-64);
    const enabledAt = this.#controlTail.lastIndexOf("\u001b[?1h");
    const disabledAt = this.#controlTail.lastIndexOf("\u001b[?1l");
    if (enabledAt >= 0 || disabledAt >= 0) {
      this.#applicationCursorKeys = enabledAt > disabledAt;
    }
    return hasStatusQuery ? [textEncoder.encode("\u001b[1;1R")] : [];
  }

  resize(dimensions: ResizePayload): void {
    this.#record({ type: "resize", dimensions: { ...dimensions } });
  }

  encodeSnapshot(): Uint8Array {
    return textEncoder.encode(
      JSON.stringify({
        applicationCursorKeys: this.#applicationCursorKeys,
        operations: this.operations.map((operation) =>
          operation.type === "output"
            ? { type: operation.type, data: [...operation.data] }
            : operation,
        ),
      }),
    );
  }

  encodeKey(input: SemanticKey): Uint8Array {
    if ((input.modifiers & KeyModifier.Control) !== 0 && input.key.length === 1) {
      const codePoint = input.key.toUpperCase().codePointAt(0);
      if (codePoint !== undefined && codePoint >= 0x40 && codePoint <= 0x5f) {
        return Uint8Array.of(codePoint - 0x40);
      }
    }

    if (input.text !== undefined) {
      return textEncoder.encode(input.text);
    }

    const encoded = keySequences[input.key];
    if (encoded === undefined) {
      throw new Error(`unsupported semantic key: ${input.key}`);
    }
    if (this.#applicationCursorKeys && input.key.startsWith("Arrow")) {
      return textEncoder.encode(encoded.replace("[", "O"));
    }
    return textEncoder.encode(encoded);
  }

  encodePaste(data: string): Uint8Array {
    const encoded = textEncoder.encode(data);
    if (encoded.byteLength > MAX_PASTE_BYTES) {
      throw new RangeError(`paste exceeds ${MAX_PASTE_BYTES} bytes`);
    }
    return encoded;
  }

  dispose(): void {}

  #record(operation: FakeAuthorityOperation): void {
    this.operations.push(operation);
    this.#operationBytes += operationSize(operation);
    while (this.#operationBytes > this.#maxOperationBytes) {
      const removed = this.operations.shift();
      if (removed === undefined) break;
      this.#operationBytes -= operationSize(removed);
    }
  }
}

function operationSize(operation: FakeAuthorityOperation): number {
  return operation.type === "output" ? operation.data.byteLength : 32;
}

const keySequences: Readonly<Record<string, string>> = {
  ArrowDown: "\u001b[B",
  ArrowLeft: "\u001b[D",
  ArrowRight: "\u001b[C",
  ArrowUp: "\u001b[A",
  Backspace: "\u007f",
  Enter: "\r",
  Escape: "\u001b",
  Tab: "\t",
};
