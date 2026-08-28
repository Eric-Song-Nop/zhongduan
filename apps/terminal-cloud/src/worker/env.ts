import type { TerminalSessionDO } from "./terminal-session-do";

export interface CloudEnv {
  ASSETS: Fetcher;
  BOOTSTRAP_TOKEN: string;
  CAPABILITY_SIGNING_KEY: string;
  CLOUD_TELEMETRY_MODE?: string;
  SNAPSHOTS: R2Bucket;
  TERMINAL_SESSIONS: DurableObjectNamespace<TerminalSessionDO>;
}
