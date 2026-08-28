import { DataFrameKind, decodeDataFrame, deliveryOutstandingBytes } from "@zhongduan/protocol";

export const JOURNAL_DEFAULTS = {
  maxAgeMs: 60_000,
  maxBytes: 8 * 1024 * 1024,
  segmentAgeMs: 250,
  segmentBytes: 256 * 1024,
} as const;

export interface EventJournalOptions {
  maxAgeMs?: number;
  maxBytes?: number;
  monotonicNow?: () => number;
  /** @deprecated Use monotonicNow. Retained for test and caller compatibility. */
  now?: () => number;
  segmentAgeMs?: number;
  segmentBytes?: number;
}

export interface JournalCursor {
  lastEventSeq: bigint;
  nextPtyOffset: bigint;
}

export type JournalReplay = { status: "ok"; frames: Uint8Array[] } | { status: "gap" };

export type JournalRangeGapReason =
  | "base-evicted"
  | "reversed"
  | "head-ahead"
  | "base-cursor-mismatch"
  | "head-cursor-mismatch";

export type JournalRangeMeasurement =
  | {
      status: "exact";
      deliveryCreditBytes: number;
      encodedBytes: number;
      frames: number;
      oldestMutationAgeMs: number;
    }
  | { status: "gap"; reason: JournalRangeGapReason };

export interface JournalMeasuredReplay {
  measurement: JournalRangeMeasurement;
  replay: JournalReplay;
}

interface JournalEntry {
  appendedAt: number;
  eventSeq: bigint;
  frame: Uint8Array;
  nextPtyOffset: bigint;
  segmentEncodedBytes: number;
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
    this.#now = options.monotonicNow ?? options.now ?? performance.now.bind(performance);
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
    const segmentEncodedBytes = segment.byteLength + stored.byteLength;
    segment.entries.push({
      appendedAt: now,
      eventSeq: frame.eventSeq,
      frame: stored,
      nextPtyOffset,
      segmentEncodedBytes,
    });
    segment.byteLength = segmentEncodedBytes;
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
    return this.#replayRange(cursor, this.#headCursor);
  }

  replayThrough(cursor: JournalCursor, commit: JournalCursor): JournalReplay {
    this.#maintain();
    return this.#replayRange(cursor, commit);
  }

  measureRange(cursor: JournalCursor, commit: JournalCursor): JournalRangeMeasurement {
    const now = this.#maintain();
    return this.#measureRange(cursor, commit, now);
  }

  /** Atomically describes and materializes one range from the same retained in-memory view. */
  replayAndMeasureThrough(cursor: JournalCursor, commit: JournalCursor): JournalMeasuredReplay {
    const now = this.#maintain();
    const measurement = this.#measureRange(cursor, commit, now);
    return {
      measurement,
      replay:
        measurement.status === "exact" ? this.#replayRange(cursor, commit) : { status: "gap" },
    };
  }

  #measureRange(
    cursor: JournalCursor,
    commit: JournalCursor,
    now: number,
  ): JournalRangeMeasurement {
    if (cursor.lastEventSeq < this.#evictedCursor.lastEventSeq) {
      return { status: "gap", reason: "base-evicted" };
    }
    if (cursor.lastEventSeq > commit.lastEventSeq) {
      return { status: "gap", reason: "reversed" };
    }
    if (commit.lastEventSeq > this.#headCursor.lastEventSeq) {
      return { status: "gap", reason: "head-ahead" };
    }

    const baseOffset = this.#offsetAfter(cursor.lastEventSeq);
    if (baseOffset === undefined || cursor.nextPtyOffset !== baseOffset) {
      return { status: "gap", reason: "base-cursor-mismatch" };
    }
    const commitOffset = this.#offsetAfter(commit.lastEventSeq);
    if (commitOffset === undefined || commit.nextPtyOffset !== commitOffset) {
      return { status: "gap", reason: "head-cursor-mismatch" };
    }

    const frames = Number(commit.lastEventSeq - cursor.lastEventSeq);
    if (!Number.isSafeInteger(frames)) {
      return { status: "gap", reason: "head-cursor-mismatch" };
    }
    const first = frames === 0 ? undefined : this.#entryLocation(cursor.lastEventSeq + 1n);
    const last = frames === 0 ? undefined : this.#entryLocation(commit.lastEventSeq);
    if (frames > 0 && (first === undefined || last === undefined)) {
      return { status: "gap", reason: "base-cursor-mismatch" };
    }
    const encodedBytes =
      first === undefined || last === undefined ? 0 : this.#encodedBytesBetween(first, last);
    const deliveryCredit = deliveryOutstandingBytes(
      { eventSeq: cursor.lastEventSeq, nextPtyOffset: cursor.nextPtyOffset },
      { eventSeq: commit.lastEventSeq, nextPtyOffset: commit.nextPtyOffset },
    );
    if (deliveryCredit > BigInt(Number.MAX_SAFE_INTEGER)) {
      throw new RangeError("journal measured range exceeds safe integer delivery credit bytes");
    }
    return {
      status: "exact",
      deliveryCreditBytes: Number(deliveryCredit),
      encodedBytes,
      frames,
      oldestMutationAgeMs: first === undefined ? 0 : Math.max(0, now - first.entry.appendedAt),
    };
  }

  #replayRange(cursor: JournalCursor, commit: JournalCursor): JournalReplay {
    if (
      cursor.lastEventSeq < this.#evictedCursor.lastEventSeq ||
      cursor.lastEventSeq > commit.lastEventSeq ||
      commit.lastEventSeq > this.#headCursor.lastEventSeq
    ) {
      return { status: "gap" };
    }

    const baseOffset = this.#offsetAfter(cursor.lastEventSeq);
    const commitOffset = this.#offsetAfter(commit.lastEventSeq);
    if (
      baseOffset === undefined ||
      cursor.nextPtyOffset !== baseOffset ||
      commitOffset === undefined ||
      commit.nextPtyOffset !== commitOffset
    ) {
      return { status: "gap" };
    }

    return {
      status: "ok",
      frames: this.#segments.flatMap((segment) =>
        segment.entries
          .filter(
            (entry) =>
              entry.eventSeq > cursor.lastEventSeq && entry.eventSeq <= commit.lastEventSeq,
          )
          .map((entry) => entry.frame.slice()),
      ),
    };
  }

  #maintain(): number {
    const now = this.#now();
    this.#sealAgedActive(now);
    this.#prune(now);
    return now;
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
    return this.#entry(eventSeq)?.nextPtyOffset;
  }

  #entry(eventSeq: bigint): JournalEntry | undefined {
    return this.#entryLocation(eventSeq)?.entry;
  }

  #entryLocation(
    eventSeq: bigint,
  ): { entry: JournalEntry; entryIndex: number; segmentIndex: number } | undefined {
    let low = 0;
    let high = this.#segments.length - 1;
    while (low <= high) {
      const middle = Math.floor((low + high) / 2);
      const entries = this.#segments[middle]!.entries;
      const first = entries[0];
      const last = entries.at(-1);
      if (first === undefined || last === undefined) return undefined;
      if (eventSeq < first.eventSeq) {
        high = middle - 1;
        continue;
      }
      if (eventSeq > last.eventSeq) {
        low = middle + 1;
        continue;
      }
      const index = Number(eventSeq - first.eventSeq);
      const entry = entries[index];
      return entry?.eventSeq === eventSeq
        ? { entry, entryIndex: index, segmentIndex: middle }
        : undefined;
    }
    return undefined;
  }

  #encodedBytesBetween(
    first: { entry: JournalEntry; entryIndex: number; segmentIndex: number },
    last: { entry: JournalEntry; entryIndex: number; segmentIndex: number },
  ): number {
    const firstSegment = this.#segments[first.segmentIndex]!;
    const beforeFirst = first.entry.segmentEncodedBytes - first.entry.frame.byteLength;
    let total: number;
    if (first.segmentIndex === last.segmentIndex) {
      total = last.entry.segmentEncodedBytes - beforeFirst;
    } else {
      total = firstSegment.byteLength - beforeFirst;
      for (
        let segmentIndex = first.segmentIndex + 1;
        segmentIndex < last.segmentIndex;
        segmentIndex += 1
      ) {
        total += this.#segments[segmentIndex]!.byteLength;
      }
      total += last.entry.segmentEncodedBytes;
    }
    if (!Number.isSafeInteger(total)) {
      throw new RangeError("journal measured range exceeds safe integer bytes");
    }
    return total;
  }
}

function positiveLimit(value: number, name: string): number {
  if (value <= 0 || Number.isNaN(value)) {
    throw new RangeError(`${name} must be positive`);
  }
  return value;
}
