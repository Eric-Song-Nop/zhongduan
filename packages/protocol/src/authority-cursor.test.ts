import { describe, expect, it } from "vitest";

import {
  AuthorityCursorSchema,
  MutationBoundarySchema,
  successorBoundary,
} from "./authority-cursor";

describe("authority cursor contracts", () => {
  it("derives the exact mutation boundary after an authority cursor", () => {
    const cursor = AuthorityCursorSchema.parse({
      sessionEpoch: "7",
      eventSeq: "19",
      nextPtyOffset: "4",
    });

    expect(successorBoundary(cursor)).toEqual({
      sessionEpoch: "7",
      nextEventSeq: "20",
      nextPtyOffset: "4",
    });
    expect(
      MutationBoundarySchema.parse({
        sessionEpoch: "7",
        nextEventSeq: "20",
        nextPtyOffset: "4",
      }),
    ).toBeDefined();
  });

  it("fails closed when an authority cursor has no uint64 successor", () => {
    expect(() =>
      successorBoundary({
        sessionEpoch: "7",
        eventSeq: "18446744073709551615",
        nextPtyOffset: "4",
      }),
    ).toThrow(RangeError);
  });
});
