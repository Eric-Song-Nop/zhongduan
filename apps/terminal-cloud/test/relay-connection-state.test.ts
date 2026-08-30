import { deriveConnectionPhase } from "../src/worker/relay-connection-state";
import { describe, expect, it } from "vitest";

describe("connection lifecycle", () => {
  it.each([
    [undefined, false, true, "reserved"],
    [undefined, false, false, "closed"],
    [false, false, false, "control-open"],
    [undefined, true, false, "data-open"],
    [false, true, false, "paired"],
    [true, true, false, "ready"],
  ] as const)(
    "maps controlReady=%s data=%s reserved=%s to %s",
    (controlReady, dataOpen, reserved, expected) => {
      expect(deriveConnectionPhase(controlReady, dataOpen, reserved)).toBe(expected);
    },
  );
});
