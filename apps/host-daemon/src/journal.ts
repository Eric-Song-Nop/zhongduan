import { DataFrameKind, decodeDataFrame } from "@zhongduan/protocol";

export const JOURNAL_DEFAULTS = {
  maxAgeMs: 60_000,
  maxBytes: 8 * 1024 * 1024,
  segmentAgeMs: 250,
  segmentBytes: 256 * 1024,
} as const;

export interface EventJournalOptions {
  maxAgeMs?: number;
  maxBytes?: number;
  now?: () => number;
  segmentAgeMs?: number;
  segmentBytes?: number;
}

export interface JournalCursor {
  lastEventSeq: bigint;
  nextPtyOffset: bigint;
}

export type JournalReplay = { status: "ok"; frames: Uint8Array[] } | { status: "gap" };

interface JournalEntry {
  eventSeq: bigint;
  frame: Uint8Array;
  nextPtyOffset: bigint;
}

interface JournalSegment {
  byteLength: number;
  createdAt: number;
  entries: JournalEntry[];
  sealed: boolean;
}

export class EventJournal {
  readonly #maxAgeMs: number;
  readonly #maxBytes: number;
  readonly #now: () => number;
  readonly #segmentAgeMs: number;
  readonly #segmentBytes: number;
  readonly #segments: JournalSegment[] = [];

  #evictedCursor: JournalCursor = { lastEventSeq: 0n, nextPtyOffset: 0n };
  #headCursor: JournalCursor = { lastEventSeq: 0n, nextPtyOffset: 0n };
  #sessionEpoch: bigint | undefined;
  #totalBytes = 0;

  constructor(options: EventJournalOptions = {}) {
    this.#maxAgeMs = positiveLimit(options.maxAgeMs ?? JOURNAL_DEFAULTS.maxAgeMs, "maxAgeMs");
    this.#maxBytes = positiveLimit(options.maxBytes ?? JOURNAL_DEFAULTS.maxBytes, "maxBytes");
    this.#now = options.now ?? Date.now;
    this.#segmentAgeMs = positiveLimit(
      options.segmentAgeMs ?? JOURNAL_DEFAULTS.segmentAgeMs,
      "segmentAgeMs",
    );
    this.#segmentBytes = positiveLimit(
      options.segmentBytes ?? JOURNAL_DEFAULTS.segmentBytes,
      "segmentBytes",
    );
  }

  get headCursor(): JournalCursor {
    return { ...this.#headCursor };
  }

  get retainedBytes(): number {
    return this.#totalBytes;
  }

  append(encoded: Uint8Array): void {
    const frame = decodeDataFrame(encoded);
    if (frame.kind !== DataFrameKind.PtyOutput && frame.kind !== DataFrameKind.ResizeApplied) {
      throw new Error("journal accepts only canonical terminal mutation frames");
    }
    if (frame.deliveryGeneration !== 0n || frame.streamId !== 0) {
      throw new Error("journal accepts only canonical delivery-neutral frames");
    }
    if (frame.eventSeq !== this.#headCursor.lastEventSeq + 1n) {
      throw new Error("journal eventSeq must be contiguous");
    }
    if (frame.ptyOffset !== this.#headCursor.nextPtyOffset) {
      throw new Error("journal PTY byte offset must be contiguous");
    }
    if (this.#sessionEpoch !== undefined && frame.sessionEpoch !== this.#sessionEpoch) {
      throw new Error("journal cannot mix session epochs");
    }
    this.#sessionEpoch ??= frame.sessionEpoch;

    const now = this.#now();
    this.#sealAgedActive(now);
    let segment = this.#segments.at(-1);
    if (segment === undefined || segment.sealed) {
      segment = { byteLength: 0, createdAt: now, entries: [], sealed: false };
      this.#segments.push(segment);
    }

    const stored = encoded.slice();
    const nextPtyOffset =
      frame.kind === DataFrameKind.PtyOutput
        ? frame.ptyOffset + BigInt(frame.payload.byteLength)
        : frame.ptyOffset;
    segment.entries.push({ eventSeq: frame.eventSeq, frame: stored, nextPtyOffset });
    segment.byteLength += stored.byteLength;
    this.#totalBytes += stored.byteLength;
    this.#headCursor = { lastEventSeq: frame.eventSeq, nextPtyOffset };

    if (segment.byteLength >= this.#segmentBytes || this.#totalBytes > this.#maxBytes) {
      segment.sealed = true;
    }
    this.#prune(now);
  }

  entries(): Uint8Array[] {
    this.#maintain();
    return this.#segments.flatMap((segment) => segment.entries.map((entry) => entry.frame.slice()));
  }

  replayFrom(cursor: JournalCursor): JournalReplay {
    this.#maintain();
    if (
      cursor.lastEventSeq < this.#evictedCursor.lastEventSeq ||
      cursor.lastEventSeq > this.#headCursor.lastEventSeq
    ) {
      return { status: "gap" };
    }

    const expectedOffset = this.#offsetAfter(cursor.lastEventSeq);
    if (expectedOffset === undefined || cursor.nextPtyOffset !== expectedOffset) {
      return { status: "gap" };
    }

    return {
      status: "ok",
      frames: this.#segments.flatMap((segment) =>
        segment.entries
          .filter((entry) => entry.eventSeq > cursor.lastEventSeq)
          .map((entry) => entry.frame.slice()),
      ),
    };
  }

  #maintain(): void {
    const now = this.#now();
    this.#sealAgedActive(now);
    this.#prune(now);
  }

  #sealAgedActive(now: number): void {
    const active = this.#segments.at(-1);
    if (active !== undefined && !active.sealed && now - active.createdAt >= this.#segmentAgeMs) {
      active.sealed = true;
    }
  }

  #prune(now: number): void {
    while (this.#segments.length > 0) {
      const oldest = this.#segments[0]!;
      const overAge = now - oldest.createdAt > this.#maxAgeMs;
      const overBytes = this.#totalBytes > this.#maxBytes;
      if (!oldest.sealed || (!overAge && !overBytes)) break;

      this.#segments.shift();
      this.#totalBytes -= oldest.byteLength;
      const last = oldest.entries.at(-1);
      if (last !== undefined) {
        this.#evictedCursor = {
          lastEventSeq: last.eventSeq,
          nextPtyOffset: last.nextPtyOffset,
        };
      }
    }
  }

  #offsetAfter(eventSeq: bigint): bigint | undefined {
    if (eventSeq === this.#evictedCursor.lastEventSeq) {
      return this.#evictedCursor.nextPtyOffset;
    }
    if (eventSeq === this.#headCursor.lastEventSeq) {
      return this.#headCursor.nextPtyOffset;
    }
    for (const segment of this.#segments) {
      const entry = segment.entries.find((candidate) => candidate.eventSeq === eventSeq);
      if (entry !== undefined) return entry.nextPtyOffset;
    }
    return undefined;
  }
}

function positiveLimit(value: number, name: string): number {
  if (value <= 0 || Number.isNaN(value)) {
    throw new RangeError(`${name} must be positive`);
  }
  return value;
}
