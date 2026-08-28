import { deriveConnectionPhase } from "../src/worker/relay-connection-state";
import { describe, expect, it } from "vitest";

describe("connection lifecycle", () => {
  it.each([
    [undefined, false, true, "reserved"],
    [undefined, false, false, "closed"],
    [null, false, false, "control-open"],
    [undefined, true, false, "data-open"],
    [null, true, false, "paired"],
    ["awaiting-attach", true, false, "paired"],
    ["active", true, false, "ready"],
  ] as const)(
    "maps control=%s data=%s reserved=%s to %s",
    (controlState, dataOpen, reserved, expected) => {
      expect(deriveConnectionPhase(controlState, dataOpen, reserved)).toBe(expected);
    },
  );
});
