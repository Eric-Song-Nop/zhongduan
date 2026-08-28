import * as nodePty from "node-pty";

import type { PtyProcess } from "./pty-process";

export interface SpawnNodePtyOptions {
  args?: string[];
  cols: number;
  command: string;
  cwd?: string;
  env?: NodeJS.ProcessEnv;
  name?: string;
  rows: number;
}

type RawPty = Omit<nodePty.IPty, "onData"> & {
  onData(listener: (data: unknown) => void): nodePty.IDisposable;
};

export function spawnNodePty(options: SpawnNodePtyOptions): PtyProcess {
  const forkOptions: nodePty.IPtyForkOptions = {
    cols: options.cols,
    encoding: null,
    env: options.env ?? process.env,
    name: options.name ?? "xterm-256color",
    rows: options.rows,
  };
  if (options.cwd !== undefined) forkOptions.cwd = options.cwd;

  const pty = nodePty.spawn(options.command, options.args ?? [], forkOptions) as RawPty;
  let exited = false;
  pty.onExit(() => {
    exited = true;
  });

  return {
    pid: pty.pid,
    onData(listener) {
      const disposable = pty.onData((data) => {
        if (!Buffer.isBuffer(data) && !(data instanceof Uint8Array)) {
          throw new TypeError("node-pty returned decoded text despite encoding: null");
        }
        listener(Uint8Array.from(data));
      });
      return () => disposable.dispose();
    },
    onExit(listener) {
      const disposable = pty.onExit(({ exitCode, signal }) => listener(exitCode, signal ?? 0));
      return () => disposable.dispose();
    },
    write(data) {
      pty.write(Buffer.from(data));
    },
    resize(dimensions) {
      pty.resize(dimensions.cols, dimensions.rows);
    },
    kill() {
      if (!exited) pty.kill();
    },
  };
}
