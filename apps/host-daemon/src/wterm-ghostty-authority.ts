import { readFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import type { ResizePayload } from "@zhongduan/protocol";
import { GHOSTTY_ENGINE_ID, GhosttyCore, GhosttyRuntime } from "@wterm/ghostty";

import type { SemanticKey, TerminalAuthority } from "./terminal-authority";

const COMMITTED_WASM_EXPORT = "@wterm/ghostty/ghostty-vt.wasm";
const PACKAGED_WASM_FILE = "ghostty-vt.wasm";
const DEFAULT_SCROLLBACK_BYTES = 8 * 1024 * 1024;
const MAX_EFFECT_FRAMES = 256;
const resolveFromPackage = createRequire(import.meta.url);
const moduleDirectory = dirname(fileURLToPath(import.meta.url));

let committedRuntime: Promise<GhosttyRuntime> | undefined;

export interface WtermGhosttyAuthorityOptions extends ResizePayload {
  scrollbackLimit?: number;
}

export async function loadCommittedGhosttyRuntime(): Promise<GhosttyRuntime> {
  if (committedRuntime !== undefined) return committedRuntime;

  const pending = loadRuntime();
  committedRuntime = pending;
  void pending.catch(() => {
    if (committedRuntime === pending) committedRuntime = undefined;
  });
  return pending;
}

export class WtermGhosttyAuthority implements TerminalAuthority {
  readonly engineId: string;
  #core: GhosttyCore | null;

  private constructor(core: GhosttyCore, engineId: string) {
    this.#core = core;
    this.engineId = engineId;
  }

  static async create(options: WtermGhosttyAuthorityOptions): Promise<WtermGhosttyAuthority> {
    const runtime = await loadCommittedGhosttyRuntime();
    const core = GhosttyCore.fromRuntime(runtime, {
      effects: "authority",
      scrollbackLimit: options.scrollbackLimit ?? DEFAULT_SCROLLBACK_BYTES,
    });

    try {
      core.init(options.cols, options.rows);
      if (options.widthPx !== 0 || options.heightPx !== 0) {
        core.resize(options.cols, options.rows, options.widthPx, options.heightPx);
      }
      const authority = new WtermGhosttyAuthority(core, runtime.engineId);
      if (authority.#drainEffects().length !== 0) {
        throw new Error("Ghostty produced an effect before the PTY was attached");
      }
      return authority;
    } catch (error) {
      core.dispose();
      throw error;
    }
  }

  applyOutput(data: Uint8Array): readonly Uint8Array[] {
    const core = this.#operationalCore();
    core.writeRaw(data);
    return this.#drainEffects();
  }

  resize(dimensions: ResizePayload): readonly Uint8Array[] {
    const core = this.#operationalCore();
    core.resize(dimensions.cols, dimensions.rows, dimensions.widthPx, dimensions.heightPx);
    return this.#drainEffects();
  }

  encodeSnapshot(): Uint8Array {
    return this.#operationalCore().encodeSnapshot();
  }

  encodeKey(key: SemanticKey): Uint8Array {
    return this.#operationalCore().encodeKey(key);
  }

  encodePaste(data: string): Uint8Array {
    return this.#operationalCore().encodePaste(data);
  }

  dispose(): void {
    const core = this.#core;
    if (core === null) return;
    this.#core = null;
    core.dispose();
  }

  #drainEffects(): readonly Uint8Array[] {
    const core = this.#operationalCore();
    const effects = core.drainEffects(MAX_EFFECT_FRAMES);
    if (effects.length === MAX_EFFECT_FRAMES && core.drainEffects(1).length !== 0) {
      throw new Error("Ghostty effect queue exceeded its pinned frame bound");
    }
    const stats = core.getEffectStats();
    if (stats.droppedFrames !== 0 || stats.droppedBytes !== 0) {
      throw new Error(
        `Ghostty effect queue overflowed: dropped ${stats.droppedFrames} frames / ${stats.droppedBytes} bytes`,
      );
    }
    return effects;
  }

  #operationalCore(): GhosttyCore {
    if (this.#core === null) {
      throw new Error("Ghostty terminal authority is disposed");
    }
    return this.#core;
  }
}

async function loadRuntime(): Promise<GhosttyRuntime> {
  const wasmPath =
    basename(moduleDirectory) === "dist"
      ? join(moduleDirectory, PACKAGED_WASM_FILE)
      : resolveFromPackage.resolve(COMMITTED_WASM_EXPORT);
  const runtime = await GhosttyRuntime.load(await readFile(wasmPath));
  if (!runtime.artifactVerified) {
    throw new Error("Ghostty runtime did not verify the committed WASM artifact");
  }
  const engineId: string = runtime.engineId;
  if (engineId !== GHOSTTY_ENGINE_ID) {
    throw new Error(`Ghostty engine mismatch: expected ${GHOSTTY_ENGINE_ID}, received ${engineId}`);
  }
  return runtime;
}
