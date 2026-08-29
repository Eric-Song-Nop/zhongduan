import { cloudflare } from "@cloudflare/vite-plugin";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const e2ePersistStatePath = process.env["ZHONGDUAN_E2E_CLOUDFLARE_STATE_PATH"];

export default defineConfig(({ mode }) => ({
  plugins: [
    react(),
    cloudflare({
      configPath: "./wrangler.jsonc",
      ...(mode === "browser-e2e" ? { config: { main: "./src/worker/browser-e2e.ts" } } : {}),
      ...(e2ePersistStatePath === undefined ? {} : { persistState: { path: e2ePersistStatePath } }),
    }),
  ],
}));
