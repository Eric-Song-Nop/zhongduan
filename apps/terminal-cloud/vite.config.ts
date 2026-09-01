import { cloudflare } from "@cloudflare/vite-plugin";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const e0PersistStatePath = process.env["ZHONGDUAN_E0_CLOUDFLARE_STATE_PATH"];

export default defineConfig(({ command, mode }) => {
  if (mode === "e0-journey") {
    if (command !== "serve") {
      throw new Error(
        "e0-journey is a local serve-only mode and cannot produce a deployable build",
      );
    }
    if (e0PersistStatePath === undefined || e0PersistStatePath.length === 0) {
      throw new Error("e0-journey requires the runner-owned temporary Cloudflare state path");
    }
  }

  return {
    plugins: [
      react(),
      cloudflare({
        configPath: "./wrangler.jsonc",
        ...(mode === "e0-journey" ? { config: { main: "./src/worker/e0-local.ts" } } : {}),
        ...(e0PersistStatePath === undefined ? {} : { persistState: { path: e0PersistStatePath } }),
      }),
    ],
  };
});
