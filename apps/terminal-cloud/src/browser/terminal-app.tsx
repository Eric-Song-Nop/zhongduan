import { RefreshCw, RotateCcw } from "lucide-react";
import { useEffect, useRef, useState, useSyncExternalStore } from "react";

import type { InputDispatcherStatus } from "./input-dispatcher";
import type { TerminalSessionSnapshot } from "./terminal-session";
import { CapabilityManager, type BrowserCapabilityBootstrap } from "./capability";
import { installTerminalDemo } from "./terminal-demo";
import { InputDispatcher } from "./input-dispatcher";
import { WTermReplicaHost } from "./replica-host";
import { TerminalSession } from "./terminal-session";
import { WorkerSnapshotTransport } from "./worker-snapshot-transport";

const EMPTY_SESSION: TerminalSessionSnapshot = {
  attempt: 0,
  controlConnected: false,
  controlOwnership: "waiting",
  dataConnected: false,
  deliveryState: "idle",
  hostOnline: true,
  lastError: null,
  phase: "idle",
  role: "writer",
};

const EMPTY_INPUT: InputDispatcherStatus = {
  connected: false,
  lastStatus: "idle",
  pending: 0,
  replicaCurrent: false,
  resizeConfirmed: false,
  writable: false,
};

interface AppRuntime {
  host: WTermReplicaHost;
  input: InputDispatcher;
  session?: TerminalSession;
}

export interface TerminalAppProps {
  capability?: BrowserCapabilityBootstrap;
  capabilityError: boolean;
  onReload?: () => void;
}

export function runRecoveryAction(
  engineMismatch: boolean,
  reconnect: () => void,
  reload: () => void,
): void {
  if (engineMismatch) reload();
  else reconnect();
}

function sessionStatusLabel(snapshot: TerminalSessionSnapshot, engineReady: boolean): string {
  if (!engineReady) return "载入终端";
  if (snapshot.lastError === "authentication") return "凭证失效";
  if (snapshot.lastError === "engine") return "引擎不匹配";
  if (snapshot.phase === "live") return "已连接";
  if (snapshot.phase === "offline") return "宿主离线";
  if (snapshot.phase === "restoring") return "恢复会话";
  if (snapshot.phase === "attaching") return "同步终端";
  if (snapshot.phase === "reconnecting") return "正在重连";
  if (snapshot.phase === "failed") return "连接失败";
  return "正在连接";
}

function useSessionSnapshot(session: TerminalSession | undefined): TerminalSessionSnapshot {
  return useSyncExternalStore(
    session === undefined ? () => () => undefined : (listener) => session.subscribe(listener),
    () => session?.snapshot ?? EMPTY_SESSION,
    () => session?.snapshot ?? EMPTY_SESSION,
  );
}

function useInputSnapshot(input: InputDispatcher | undefined): InputDispatcherStatus {
  return useSyncExternalStore(
    input === undefined ? () => () => undefined : (listener) => input.subscribe(listener),
    () => input?.status ?? EMPTY_INPUT,
    () => input?.status ?? EMPTY_INPUT,
  );
}

export function TerminalApp({ capability, capabilityError, onReload }: TerminalAppProps) {
  const terminalRef = useRef<HTMLDivElement>(null);
  const sessionRef = useRef<TerminalSession | null>(null);
  const [runtime, setRuntime] = useState<AppRuntime>();
  const [engineFailed, setEngineFailed] = useState(false);
  const [title, setTitle] = useState("Terminal");
  const [adoptions, setAdoptions] = useState(0);
  const session = useSessionSnapshot(runtime?.session);
  const input = useInputSnapshot(runtime?.input);
  const demo =
    import.meta.env.DEV && new URLSearchParams(window.location.search).get("demo") === "1";

  useEffect(() => {
    const element = terminalRef.current;
    if (element === null) return;
    let cancelled = false;
    let ownedRuntime: AppRuntime | null = null;

    void (async () => {
      const dispatcher = new InputDispatcher({
        getObservedEventSeq: () => sessionRef.current?.activeCursor?.lastEventSeq ?? null,
      });
      let host: WTermReplicaHost;
      try {
        host = await WTermReplicaHost.create({
          element,
          inputSink: dispatcher,
          onAdopt: () => setAdoptions((count) => count + 1),
          onAuthoritativeResize: (dimensions) => {
            dispatcher.confirmAuthoritativeResize(dimensions);
          },
          onTitle: (nextTitle) => setTitle(nextTitle || "Terminal"),
        });
      } catch {
        if (!cancelled) setEngineFailed(true);
        return;
      }
      if (cancelled) {
        host.dispose();
        return;
      }

      ownedRuntime = { host, input: dispatcher };
      if (demo) {
        setRuntime(ownedRuntime);
        await installTerminalDemo(host);
        return;
      }
      if (capability === undefined) {
        host.active?.writePty(
          new TextEncoder().encode("\x1b[2J\x1b[H\r\n  Session capability required.\r\n"),
        );
        setRuntime(ownedRuntime);
        return;
      }

      const capabilities = new CapabilityManager({
        bootstrap: capability,
        storage: window.sessionStorage,
      });
      const snapshots = new WorkerSnapshotTransport({
        getCapability: () => capabilities.capability,
      });
      const nextSession = new TerminalSession({
        capabilities,
        engineId: host.engineId,
        host,
        input: dispatcher,
        sessionId: capability.sessionId,
        snapshots,
        storage: window.sessionStorage,
      });
      sessionRef.current = nextSession;
      ownedRuntime = { host, input: dispatcher, session: nextSession };
      setRuntime(ownedRuntime);
      nextSession.start();
    })();

    return () => {
      cancelled = true;
      if (demo) delete window.__zhongduanE2E;
      sessionRef.current = null;
      ownedRuntime?.session?.close();
      ownedRuntime?.host.dispose();
    };
  }, [capability, demo]);

  useEffect(() => {
    document.title = title === "Terminal" ? "Zhongduan" : `${title} - Zhongduan`;
  }, [title]);

  const engineReady = runtime !== undefined && !engineFailed;
  const statusLabel = demo
    ? "本地终端"
    : capabilityError && capability === undefined
      ? "缺少凭证"
      : sessionStatusLabel(session, engineReady);
  const statusTone =
    demo || session.phase === "live"
      ? "ok"
      : engineFailed || session.phase === "failed" || session.lastError !== null
        ? "error"
        : session.phase === "offline"
          ? "warning"
          : "busy";
  const ownershipLabel =
    demo || session.controlOwnership === "writer"
      ? "已控制"
      : session.controlOwnership === "observer"
        ? "只读"
        : "获取控制";
  const engineMismatch = session.lastError === "engine";
  const recoveryLabel = engineMismatch ? "重新载入兼容终端" : "重新连接";

  return (
    <main
      className="terminal-app"
      data-adoptions={adoptions}
      data-phase={demo ? "demo" : session.phase}
    >
      <header className="session-bar">
        <div className="product-mark" aria-label="Zhongduan">
          <span className="product-glyph" aria-hidden="true">
            Z
          </span>
          <span className="product-name">Zhongduan</span>
        </div>
        <div className="connection-status" aria-live="polite">
          <span className="status-light" data-tone={statusTone} aria-hidden="true" />
          <span>{statusLabel}</span>
        </div>
        <span className="terminal-title" title={title}>
          {title}
        </span>
        <div className="session-actions">
          {input.pending > 0 ? <span className="input-state">输入 {input.pending}</span> : null}
          <button
            className="ownership-button"
            data-owned={demo || session.controlOwnership === "writer"}
            disabled={
              demo ||
              engineMismatch ||
              session.controlOwnership === "observer" ||
              runtime?.session === undefined
            }
            onClick={() => runtime?.session?.reconnectNow()}
            title={session.controlOwnership === "waiting" ? "重新连接并申请控制权" : ownershipLabel}
            type="button"
          >
            {ownershipLabel}
          </button>
          <button
            aria-label={recoveryLabel}
            className="recovery-button"
            disabled={runtime?.session === undefined}
            onClick={() =>
              runRecoveryAction(
                engineMismatch,
                () => runtime?.session?.reconnectNow(),
                onReload ?? (() => window.location.reload()),
              )
            }
            title={recoveryLabel}
            type="button"
          >
            {engineMismatch ? (
              <RotateCcw aria-hidden="true" size={15} strokeWidth={1.8} />
            ) : (
              <RefreshCw aria-hidden="true" size={15} strokeWidth={1.8} />
            )}
          </button>
        </div>
      </header>
      <section className="terminal-stage" aria-label="Remote terminal session">
        <div ref={terminalRef} className="terminal-viewport" data-testid="wterm-surface" />
        {!engineReady && !engineFailed ? <div className="engine-status">正在载入终端</div> : null}
        {engineFailed ? <div className="engine-status error">终端引擎加载失败</div> : null}
      </section>
    </main>
  );
}
