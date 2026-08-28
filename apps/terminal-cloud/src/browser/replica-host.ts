import type { InputSink } from "@wterm/core";
import { WTerm } from "@wterm/dom";
import {
  GhosttyCore,
  GhosttyRuntime,
  type GhosttyPassiveRestore,
  type GhosttyWasmSource,
} from "@wterm/ghostty";
import type { ReplicaCursor, ResizePayload } from "@zhongduan/protocol";
import type { ReplicaHost, ReplicaSink, SnapshotManifest } from "@zhongduan/session-client";

const MAX_CONTINUATION_BYTES = 64 * 1024;

export interface WTermReplicaHostOptions {
  element: HTMLElement;
  inputSink: InputSink;
  wasmSource?: GhosttyWasmSource;
  onAdopt?: (cursor: ReplicaCursor) => void;
  onAuthoritativeResize?: (dimensions: ResizePayload) => void;
  onTitle?: (title: string) => void;
}

class GhosttyReplicaSink implements ReplicaSink {
  readonly engineId: string;
  #core: GhosttyCore | null;
  #term: WTerm | null = null;
  #disposed = false;
  #lastResize: ResizePayload | null = null;

  constructor(
    engineId: string,
    core: GhosttyCore,
    private readonly onVisibleResize: (dimensions: ResizePayload) => void,
  ) {
    this.engineId = engineId;
    this.#core = core;
  }

  get lastResize(): ResizePayload | null {
    return this.#lastResize === null ? null : { ...this.#lastResize };
  }

  writePty(data: Uint8Array): void {
    this.#assertLive();
    if (this.#term !== null) this.#term.write(data);
    else this.#core!.writeRaw(data);
  }

  resize(dimensions: ResizePayload): void {
    this.#assertLive();
    this.#lastResize = { ...dimensions };
    const { cols, rows, widthPx, heightPx } = dimensions;
    if (this.#term !== null) {
      this.#term.resize(cols, rows, widthPx, heightPx);
      this.onVisibleResize({ ...dimensions });
    } else {
      this.#core!.resize(cols, rows, widthPx, heightPx);
    }
  }

  adoptInto(term: WTerm): void {
    this.#assertLive();
    if (this.#term !== null) throw new Error("replica is already visible");
    const core = this.#core!;
    term.adoptCore(core);
    this.#core = null;
    this.#term = term;
  }

  releaseAfterReplacement(): void {
    this.#core = null;
    this.#term = null;
    this.#disposed = true;
  }

  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    if (this.#term === null) this.#core?.dispose();
    this.#core = null;
    this.#term = null;
  }

  #assertLive(): void {
    if (this.#disposed || (this.#core === null && this.#term === null)) {
      throw new Error("replica is disposed");
    }
  }
}

/** Owns the one visible WTerm core and at most one detached restore candidate. */
export class WTermReplicaHost implements ReplicaHost {
  readonly engineId: string;
  readonly #runtime: GhosttyRuntime;
  readonly #term: WTerm;
  readonly #onAdopt?: WTermReplicaHostOptions["onAdopt"];
  readonly #onAuthoritativeResize: (dimensions: ResizePayload) => void;
  #active: GhosttyReplicaSink | null;
  #candidate: GhosttyReplicaSink | null = null;
  #restoreHandle: GhosttyPassiveRestore | null = null;
  #restoreAttempt = 0;
  #disposed = false;

  private constructor(
    runtime: GhosttyRuntime,
    term: WTerm,
    initial: GhosttyReplicaSink,
    options: WTermReplicaHostOptions,
  ) {
    this.#runtime = runtime;
    this.#term = term;
    this.#active = initial;
    this.engineId = runtime.engineId;
    this.#onAdopt = options.onAdopt;
    this.#onAuthoritativeResize = options.onAuthoritativeResize ?? (() => undefined);
  }

  static async create(options: WTermReplicaHostOptions): Promise<WTermReplicaHost> {
    const runtime = await GhosttyRuntime.load(options.wasmSource);
    const core = GhosttyCore.fromRuntime(runtime, {
      effects: "discard",
      foregroundColor: "#d8dee9",
      backgroundColor: "#111315",
      scrollbackLimit: 8 * 1024 * 1024,
    });
    let host: WTermReplicaHost | null = null;
    const initial = new GhosttyReplicaSink(runtime.engineId, core, (dimensions) => {
      if (host !== null) host.#onAuthoritativeResize(dimensions);
    });
    const term = new WTerm(options.element, {
      autoResize: true,
      core,
      cursorBlink: true,
      inputSink: options.inputSink,
      ...(options.onTitle === undefined ? {} : { onTitle: options.onTitle }),
    });
    try {
      await term.init();
      initial.adoptInto(term);
      host = new WTermReplicaHost(runtime, term, initial, options);
      return host;
    } catch (error) {
      term.destroy();
      initial.dispose();
      throw error;
    }
  }

  get active(): ReplicaSink | null {
    return this.#active;
  }

  focus(): void {
    this.#term.focus();
  }

  async restore(
    snapshot: Uint8Array,
    manifest: SnapshotManifest,
    signal: AbortSignal,
  ): Promise<ReplicaSink> {
    this.#assertLive();
    if (manifest.engineId !== this.engineId) {
      throw new Error("snapshot engine does not match the loaded Ghostty runtime");
    }
    if (manifest.restoreThrough !== "finish") {
      throw new Error("browser replicas require a complete history restore");
    }
    signal.throwIfAborted();

    const attempt = ++this.#restoreAttempt;
    this.#disposeDetached();
    const restore = this.#runtime.beginPassiveRestore(snapshot, {
      effects: "discard",
      maxContinuationBytes: MAX_CONTINUATION_BYTES,
    });
    this.#restoreHandle = restore;
    try {
      await restore.advanceToFinish({ signal, yieldBetweenPages: true });
      signal.throwIfAborted();
      if (attempt !== this.#restoreAttempt || this.#disposed) {
        throw new DOMException("restore was replaced", "AbortError");
      }
      const core = restore.takeCore();
      this.#restoreHandle = null;
      restore.dispose();
      const candidate = new GhosttyReplicaSink(this.engineId, core, (dimensions) => {
        this.#onAuthoritativeResize(dimensions);
      });
      this.#candidate = candidate;
      return candidate;
    } catch (error) {
      if (this.#restoreHandle === restore) this.#restoreHandle = null;
      restore.dispose();
      throw error;
    }
  }

  adopt(replica: ReplicaSink, cursor: ReplicaCursor): void {
    this.#assertLive();
    if (!(replica instanceof GhosttyReplicaSink) || replica !== this.#candidate) {
      throw new Error("replica host can only adopt its current detached candidate");
    }
    if (replica.engineId !== this.engineId) {
      throw new Error("candidate engine does not match the visible terminal");
    }
    const previous = this.#active;
    replica.adoptInto(this.#term);
    previous?.releaseAfterReplacement();
    this.#active = replica;
    this.#candidate = null;
    const dimensions = replica.lastResize;
    if (dimensions !== null) this.#onAuthoritativeResize(dimensions);
    this.#onAdopt?.({ ...cursor });
  }

  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    ++this.#restoreAttempt;
    this.#disposeDetached();
    this.#term.destroy();
    this.#active?.releaseAfterReplacement();
    this.#active = null;
  }

  #disposeDetached(): void {
    this.#restoreHandle?.dispose();
    this.#restoreHandle = null;
    this.#candidate?.dispose();
    this.#candidate = null;
  }

  #assertLive(): void {
    if (this.#disposed) throw new Error("replica host is disposed");
  }
}
