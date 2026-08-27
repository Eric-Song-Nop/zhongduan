import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    deps: {
      alwaysBundle: ["@zhongduan/protocol"],
      neverBundle: ["node-pty"],
      onlyBundle: ["zod"],
    },
    entry: ["src/index.ts", "src/cli.ts"],
    format: ["esm"],
    platform: "node",
    sourcemap: true,
  },
});
