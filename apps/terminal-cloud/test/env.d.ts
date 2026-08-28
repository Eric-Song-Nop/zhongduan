import type { CloudEnv } from "../src/worker/env";

declare global {
  namespace Cloudflare {
    interface Env extends CloudEnv {}

    interface GlobalProps {
      mainModule: typeof import("../src/worker/index");
      durableNamespaces: "TerminalSessionDO";
    }
  }
}

export {};
