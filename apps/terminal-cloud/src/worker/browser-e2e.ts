import productionWorker, { TerminalSessionDO as ProductionTerminalSessionDO } from "./index";
import type { CloudEnv } from "./env";
import { installMiniflareMultipartEtagCompatibility } from "./miniflare-snapshot-compat";

const LOOPBACK_HOSTS = new Set(["127.0.0.1", "[::1]", "localhost"]);

export function isBrowserE2ELoopbackRequest(request: Request): boolean {
  return LOOPBACK_HOSTS.has(new URL(request.url).hostname);
}

export class TerminalSessionDO extends ProductionTerminalSessionDO {
  constructor(ctx: DurableObjectState, env: CloudEnv) {
    super(ctx, env);
    installMiniflareMultipartEtagCompatibility(this);
  }
}

export default {
  fetch(request, env) {
    if (!isBrowserE2ELoopbackRequest(request)) {
      return Response.json(
        { error: "not-found" },
        { status: 404, headers: { "cache-control": "no-store" } },
      );
    }
    return productionWorker.fetch(request, env);
  },
} satisfies ExportedHandler<CloudEnv>;
