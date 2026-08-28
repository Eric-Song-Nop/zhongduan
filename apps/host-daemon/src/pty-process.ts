import type { ResizePayload } from "@zhongduan/protocol";

export interface PtyProcess {
  readonly pid: number;

  onData(listener: (data: Uint8Array) => void): () => void;
  onExit(listener: (exitCode: number, signal: number) => void): () => void;
  write(data: Uint8Array): void;
  resize(dimensions: ResizePayload): void;
  kill(): void;
}
