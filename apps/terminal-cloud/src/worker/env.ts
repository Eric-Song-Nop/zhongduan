import type { TerminalSessionDO } from "./terminal-session-do";

export interface CloudEnv {
  ASSETS: Fetcher;
  BOOTSTRAP_TOKEN: string;
  CAPABILITY_SIGNING_KEY: string;
  RECOVERY_V3_ENABLED?: "false" | "true";
  SNAPSHOTS: R2Bucket;
  TERMINAL_SESSIONS: DurableObjectNamespace<TerminalSessionDO>;
}
