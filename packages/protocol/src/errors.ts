export type ProtocolErrorCode =
  | "BAD_MAGIC"
  | "BAD_VERSION"
  | "BAD_KIND"
  | "BAD_FLAGS"
  | "BAD_LENGTH"
  | "BAD_PAYLOAD"
  | "BAD_EPOCH"
  | "BAD_GENERATION"
  | "EVENT_GAP"
  | "PTY_OFFSET_GAP"
  | "OUT_OF_RANGE";

export class ProtocolError extends Error {
  readonly code: ProtocolErrorCode;

  constructor(code: ProtocolErrorCode, message: string) {
    super(message);
    this.name = "ProtocolError";
    this.code = code;
  }
}
