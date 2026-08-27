import { describe, expect, it } from "vitest";

import {
  DELIVERY_EVENT_OVERHEAD_BYTES,
  MAX_DELIVERY_OUTSTANDING_BYTES,
  deliveryOutstandingBytes,
} from "./delivery-credit";

describe("delivery credit", () => {
  it("uses the shared 512 KiB cap and 64-byte event charge", () => {
    expect(MAX_DELIVERY_OUTSTANDING_BYTES).toBe(512 * 1024);
    expect(DELIVERY_EVENT_OVERHEAD_BYTES).toBe(64n);
    expect(
      deliveryOutstandingBytes(
        { eventSeq: 10n, nextPtyOffset: 1_000n },
        { eventSeq: 13n, nextPtyOffset: 1_100n },
      ),
    ).toBe(292n);
  });

  it("rejects reversed or negative cursors", () => {
    expect(() =>
      deliveryOutstandingBytes(
        { eventSeq: 2n, nextPtyOffset: 2n },
        { eventSeq: 1n, nextPtyOffset: 2n },
      ),
    ).toThrow(RangeError);
    expect(() =>
      deliveryOutstandingBytes(
        { eventSeq: -1n, nextPtyOffset: 0n },
        { eventSeq: 0n, nextPtyOffset: 0n },
      ),
    ).toThrow(RangeError);
  });
});
