import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      wrangler: { configPath: "./wrangler.jsonc" },
      miniflare: {
        bindings: {
          BOOTSTRAP_TOKEN: "test-bootstrap-token-with-at-least-32-bytes",
          CAPABILITY_SIGNING_KEY: "test-capability-key-with-at-least-32-bytes",
          CLOUD_TELEMETRY_MODE: "off",
        },
      },
    }),
  ],
  test: {
    include: ["test/**/*.test.ts"],
    isolate: false,
    maxWorkers: 1,
  },
});
