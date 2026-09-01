import productionWorker, { TerminalSessionDO as ProductionTerminalSessionDO } from "./index";
import type { CloudEnv } from "./env";

const LOOPBACK_HOSTS = new Set(["127.0.0.1", "[::1]", "localhost"]);

/** The E0 entry is test-only and cannot become an accidentally deployed public Worker. */
export function isE0LoopbackRequest(request: Request): boolean {
  return LOOPBACK_HOSTS.has(new URL(request.url).hostname);
}

/**
 * Miniflare uses opaque multipart part ETags while production R2 returns the part MD5. The
 * production coordinator deliberately verifies that MD5 before completing an upload, so the
 * loopback-only E0 runtime scopes the existing compatibility shim to its own Durable Object
 * instances instead of weakening production verification.
 */
export function installE0MiniflareMultipartEtagShim(coordinator: object): void {
  Object.defineProperty(coordinator, "snapshotPartEtagMatches", {
    configurable: true,
    value: () => true,
  });
}

export class TerminalSessionDO extends ProductionTerminalSessionDO {
  constructor(ctx: DurableObjectState, env: CloudEnv) {
    super(ctx, env);
    const coordinator = Reflect.get(this, "snapshotUploads") as unknown;
    if (typeof coordinator !== "object" || coordinator === null) {
      throw new Error("E0 snapshot upload coordinator is unavailable");
    }
    installE0MiniflareMultipartEtagShim(coordinator);
  }
}

export default {
  fetch(request, env) {
    if (!isE0LoopbackRequest(request)) {
      return Response.json(
        { error: "not-found" },
        { status: 404, headers: { "cache-control": "no-store" } },
      );
    }
    return productionWorker.fetch(request, env);
  },
} satisfies ExportedHandler<CloudEnv>;
