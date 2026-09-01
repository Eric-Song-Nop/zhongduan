#!/usr/bin/env node

import { performance } from "node:perf_hooks";
import { readFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";

import { GhosttyCore, GhosttyRuntime } from "../vendor/wterm/packages/@wterm/ghostty/dist/index.js";

const root = new URL("../", import.meta.url);
const wasmUrl = new URL("vendor/wterm/packages/@wterm/ghostty/wasm/ghostty-vt.wasm", root);
const encoder = new TextEncoder();

function stableHash(value) {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

function normalizeCell(cell) {
  return {
    char: cell.char,
    chars: cell.chars ?? null,
    width: cell.width,
    fg: cell.fg,
    bg: cell.bg,
    fgRgb: cell.fgRgb ?? null,
    bgRgb: cell.bgRgb ?? null,
    flags: cell.flags,
    linkKey: cell.linkKey ?? null,
    linkUri: cell.linkUri ?? null,
  };
}

function normalizedState(core) {
  const rows = core.getRows();
  const cols = core.getCols();
  const viewport = [];
  for (let row = 0; row < rows; row += 1) {
    const cells = [];
    for (let col = 0; col < cols; col += 1) cells.push(normalizeCell(core.getCell(row, col)));
    viewport.push(cells);
  }
  const scrollback = [];
  for (let offset = 0; offset < core.getScrollbackCount(); offset += 1) {
    const cells = [];
    const length = core.getScrollbackLineLen(offset);
    for (let col = 0; col < length; col += 1) {
      cells.push(normalizeCell(core.getScrollbackCell(offset, col)));
    }
    scrollback.push(cells);
  }
  return {
    cols,
    rows,
    viewport,
    scrollback,
    cursor: core.getCursor(),
    modes: {
      bracketedPaste: core.bracketedPaste(),
      cursorKeysApp: core.cursorKeysApp(),
      focusEvents: core.focusEvents(),
      mouseSgr: core.mouseSgr(),
      mouseTracking: core.mouseTracking(),
      synchronizedOutput: core.synchronizedOutput(),
      synchronizedOutputGeneration: core.synchronizedOutputGeneration(),
      usingAltScreen: core.usingAltScreen(),
    },
    continuation: Array.from(core.getContinuation()),
    effectRelevantEncoders: {
      arrowUp: Array.from(core.encodeKey({ key: "ArrowUp" })),
      paste: Array.from(core.encodePaste("e0-paste")),
      focusIn: Array.from(core.encodeFocus(true)),
      focusOut: Array.from(core.encodeFocus(false)),
    },
    effectStats: core.getEffectStats(),
    unhandledSequences: core.getUnhandledSequences(),
  };
}

function bytes(value) {
  return typeof value === "string" ? encoder.encode(value) : value;
}

function applyActions(core, actions) {
  for (const action of actions) {
    if (action.type === "write") core.writeRaw(bytes(action.value));
    else if (action.type === "resize")
      core.resize(action.cols, action.rows, action.widthPx, action.heightPx);
    else throw new Error(`unknown authority corpus action ${action.type}`);
  }
}

async function compareSnapshotCase(runtime, testCase) {
  const createCore = () => {
    const core = GhosttyCore.fromRuntime(runtime, {
      effects: "discard",
      scrollbackLimit: 8 * 1024 * 1024,
    });
    core.init(testCase.cols ?? 80, testCase.rows ?? 24);
    return core;
  };
  const uninterrupted = createCore();
  const checkpointSource = createCore();
  applyActions(uninterrupted, testCase.prefix);
  applyActions(checkpointSource, testCase.prefix);
  const beforeCapture = normalizedState(checkpointSource);
  const snapshot = checkpointSource.encodeSnapshot();
  const afterCapture = normalizedState(checkpointSource);
  const restore = runtime.beginPassiveRestore(snapshot, {
    effects: "discard",
    maxContinuationBytes: 64 * 1024,
  });
  await restore.advanceToFinish({ yieldBetweenPages: true });
  const recovered = restore.takeCore();
  restore.dispose();
  applyActions(uninterrupted, testCase.suffix);
  applyActions(checkpointSource, testCase.suffix);
  applyActions(recovered, testCase.suffix);
  const uninterruptedState = normalizedState(uninterrupted);
  const checkpointSourceState = normalizedState(checkpointSource);
  const recoveredState = normalizedState(recovered);
  const snapshotCaptureStateEqual = stableHash(beforeCapture) === stableHash(afterCapture);
  const checkpointSourceStateEqual =
    stableHash(uninterruptedState) === stableHash(checkpointSourceState);
  const recoveredStateEqual = stableHash(uninterruptedState) === stableHash(recoveredState);
  const checkpointSourceContinuationEqual =
    stableHash(uninterruptedState.continuation) === stableHash(checkpointSourceState.continuation);
  const recoveredContinuationEqual =
    stableHash(uninterruptedState.continuation) === stableHash(recoveredState.continuation);
  const result = {
    id: testCase.id,
    coveredFields: testCase.coveredFields,
    snapshotBytes: snapshot.byteLength,
    snapshotCaptureStateEqual,
    checkpointSourceStateEqual,
    recoveredStateEqual,
    normalizedStateEqual:
      snapshotCaptureStateEqual && checkpointSourceStateEqual && recoveredStateEqual,
    checkpointSourceContinuationEqual,
    recoveredContinuationEqual,
    continuationEqual: checkpointSourceContinuationEqual && recoveredContinuationEqual,
    beforeCaptureStateSha256: stableHash(beforeCapture),
    afterCaptureStateSha256: stableHash(afterCapture),
    uninterruptedStateSha256: stableHash(uninterruptedState),
    checkpointSourceStateSha256: stableHash(checkpointSourceState),
    recoveredStateSha256: stableHash(recoveredState),
    uninterruptedContinuationSha256: stableHash(uninterruptedState.continuation),
    checkpointSourceContinuationSha256: stableHash(checkpointSourceState.continuation),
    recoveredContinuationSha256: stableHash(recoveredState.continuation),
  };
  uninterrupted.dispose();
  checkpointSource.dispose();
  recovered.dispose();
  return result;
}

function concatBytes(chunks) {
  const values = chunks.map(bytes);
  const output = new Uint8Array(values.reduce((total, value) => total + value.byteLength, 0));
  let offset = 0;
  for (const value of values) {
    output.set(value, offset);
    offset += value.byteLength;
  }
  return output;
}

function compareEffectCase(runtime, testCase) {
  const uninterrupted = GhosttyCore.fromRuntime(runtime, { effects: "authority" });
  const split = GhosttyCore.fromRuntime(runtime, { effects: "authority" });
  uninterrupted.init(80, 24);
  split.init(80, 24);
  uninterrupted.writeRaw(concatBytes(testCase.chunks));
  for (const chunk of testCase.chunks) split.writeRaw(bytes(chunk));
  const uninterruptedEffects = uninterrupted.drainEffects().map((value) => Array.from(value));
  const splitEffects = split.drainEffects().map((value) => Array.from(value));
  const result = {
    id: testCase.id,
    coveredFields: testCase.coveredFields,
    effectsEqual: stableHash(uninterruptedEffects) === stableHash(splitEffects),
    uninterruptedEffectsSha256: stableHash(uninterruptedEffects),
    splitEffectsSha256: stableHash(splitEffects),
    effectFrameCount: uninterruptedEffects.length,
  };
  uninterrupted.dispose();
  split.dispose();
  return result;
}

function p99(values) {
  const ordered = values.toSorted((left, right) => left - right);
  return ordered[Math.max(0, Math.ceil(ordered.length * 0.99) - 1)];
}

async function measureSnapshotPause(core, samples) {
  const disabled = [];
  const enabled = [];
  for (let index = 0; index < samples; index += 1) {
    let startedAt = performance.now();
    disabled.push(
      await new Promise((resolve) => queueMicrotask(() => resolve(performance.now() - startedAt))),
    );

    startedAt = performance.now();
    const waiting = new Promise((resolve) =>
      queueMicrotask(() => resolve(performance.now() - startedAt)),
    );
    core.encodeSnapshot();
    enabled.push(await waiting);
  }
  return {
    clock: "node-performance-now",
    samples,
    snapshotDisabledMs: disabled,
    snapshotEnabledMs: enabled,
    p99: {
      snapshotDisabledMs: p99(disabled),
      snapshotEnabledMs: p99(enabled),
    },
  };
}

async function main() {
  const arguments_ = process.argv.slice(2);
  const allowFailure = arguments_.includes("--allow-failure");
  if (arguments_.some((argument) => argument !== "--allow-failure"))
    throw new Error(`unknown authority oracle argument: ${arguments_.join(" ")}`);
  const samples = Number.parseInt(process.env.ZHONGDUAN_E0_SAMPLES ?? "24", 10);
  if (!Number.isSafeInteger(samples) || samples <= 0)
    throw new RangeError("sample count must be positive");
  const runtime = await GhosttyRuntime.load(await readFile(wasmUrl));
  if (!runtime.artifactVerified) throw new Error("committed Ghostty WASM did not verify");
  const history = Array.from(
    { length: 160 },
    (_, index) => `history-${index.toString().padStart(4, "0")}\r\n`,
  ).join("");
  const smile = encoder.encode("🙂");
  const snapshotCases = [
    {
      id: "history-modes-alt-screen-incomplete-csi",
      coveredFields: [
        "screen",
        "history",
        "cursor",
        "modes",
        "alternate-screen",
        "parser-continuation",
        "cell-colors-and-flags",
        "effect-relevant-encoders",
      ],
      prefix: [
        {
          type: "write",
          value: `\x1b[2J\x1b[H${history}\x1b[?1h\x1b[?1004h\x1b[?2004h\x1b[38;2;12;34;56`,
        },
      ],
      suffix: [
        {
          type: "write",
          value: "mCONTINUATION\r\n\x1b[?1049hALT\x1b[?1049lPRIMARY\r\n",
        },
      ],
    },
    {
      id: "split-utf8-grapheme",
      coveredFields: ["parser-continuation", "utf8-decoder", "grapheme", "cell-width"],
      prefix: [{ type: "write", value: smile.slice(0, 2) }],
      suffix: [{ type: "write", value: concatBytes([smile.slice(2), "界e\u0301\r\n"]) }],
    },
    {
      id: "split-osc-and-hyperlink",
      coveredFields: ["parser-continuation", "osc", "title", "hyperlink-cell-metadata"],
      prefix: [{ type: "write", value: "\x1b]2;E0 partial title" }],
      suffix: [
        {
          type: "write",
          value: "\x07\x1b]8;;https://example.invalid/e0\x07LINK\x1b]8;;\x07\r\n",
        },
      ],
    },
    {
      id: "margins-tabs-palette-charset-pending-wrap",
      cols: 10,
      rows: 5,
      coveredFields: ["margins", "tab-stops", "palette", "charset", "pending-wrap", "cursor"],
      prefix: [
        {
          type: "write",
          value: "\x1b[2;4r\x1b[3g\x1bH\x1b]4;1;rgb:ff/00/00\x07\x1b(0\x1b[H1234567890",
        },
      ],
      suffix: [{ type: "write", value: "X\x1b(B\tEND" }],
    },
    {
      id: "resize-geometry-and-scrollback",
      coveredFields: ["resize", "geometry", "screen", "history", "cursor"],
      prefix: [
        { type: "write", value: "before-resize-1\r\nbefore-resize-2\r\n" },
        { type: "resize", cols: 40, rows: 12, widthPx: 800, heightPx: 480 },
      ],
      suffix: [
        { type: "write", value: "after-resize\r\n" },
        { type: "resize", cols: 72, rows: 20, widthPx: 1440, heightPx: 800 },
      ],
    },
  ];
  const corpus = [];
  for (const testCase of snapshotCases) corpus.push(await compareSnapshotCase(runtime, testCase));
  const effectCorpus = [
    compareEffectCase(runtime, {
      id: "split-cursor-position-query",
      coveredFields: ["parser-continuation", "write-pty-effects", "cursor-position-query"],
      chunks: ["\x1b[", "6n"],
    }),
    compareEffectCase(runtime, {
      id: "split-device-attributes-query",
      coveredFields: ["parser-continuation", "write-pty-effects", "device-attributes"],
      chunks: ["\x1b[>", "c"],
    }),
    compareEffectCase(runtime, {
      id: "focus-and-mode-effect-state",
      coveredFields: ["modes", "effect-relevant-encoders"],
      chunks: ["\x1b[?1004", "h\x1b[?2004h"],
    }),
  ];
  const diagnosticCore = GhosttyCore.fromRuntime(runtime, { effects: "discard" });
  diagnosticCore.init(80, 24);
  diagnosticCore.writeRaw(encoder.encode(history));
  const pause = await measureSnapshotPause(diagnosticCore, samples);
  const snapshotCaptureStateEqual = corpus.every((item) => item.snapshotCaptureStateEqual);
  const checkpointSourceStateEqual = corpus.every((item) => item.checkpointSourceStateEqual);
  const recoveredStateEqual = corpus.every((item) => item.recoveredStateEqual);
  const normalizedStateEqual =
    snapshotCaptureStateEqual && checkpointSourceStateEqual && recoveredStateEqual;
  const checkpointSourceContinuationEqual = corpus.every(
    (item) => item.checkpointSourceContinuationEqual,
  );
  const recoveredContinuationEqual = corpus.every((item) => item.recoveredContinuationEqual);
  const continuationEqual = checkpointSourceContinuationEqual && recoveredContinuationEqual;
  const effectsEqual = effectCorpus.every((item) => item.effectsEqual);
  const gitOutput = (...arguments_) =>
    execFileSync("git", arguments_, { cwd: root, encoding: "utf8" }).trim();

  const result = {
    schemaVersion: "zhongduan-e0-authority-oracle-v3",
    sourceRevision: gitOutput("rev-parse", "HEAD"),
    sourceTreeGitOid: gitOutput("rev-parse", "HEAD^{tree}"),
    sourceTreeDirty: gitOutput("status", "--porcelain=v1", "--untracked-files=all") !== "",
    engineId: runtime.engineId,
    artifactVerified: runtime.artifactVerified,
    corpus,
    effectCorpus: {
      scope:
        "authority WRITE_PTY effects under equivalent whole-vs-byte-split canonical output; passive snapshot restore intentionally emits no effects",
      cases: effectCorpus,
      effectsEqual,
    },
    coveredFields: [...new Set(corpus.flatMap((item) => item.coveredFields))].toSorted(
      (left, right) => left.localeCompare(right),
    ),
    snapshotBytes: corpus.reduce((total, item) => total + item.snapshotBytes, 0),
    comparison: {
      sampleId: "ghostty-snapshot-on-off",
      snapshotCaptureStateEqual,
      checkpointSourceStateEqual,
      recoveredStateEqual,
      normalizedStateEqual,
      checkpointSourceContinuationEqual,
      recoveredContinuationEqual,
      continuationEqual,
      effectsEqual,
      corpusCaseCount: corpus.length,
      effectCaseCount: effectCorpus.length,
      corpusSha256: stableHash(corpus),
      effectCorpusSha256: stableHash(effectCorpus),
    },
    snapshotPause: pause,
  };
  diagnosticCore.dispose();
  process.stdout.write(`${JSON.stringify(result)}\n`);
  if (!allowFailure && (!normalizedStateEqual || !continuationEqual || !effectsEqual)) {
    process.exitCode = 1;
  }
}

await main();
