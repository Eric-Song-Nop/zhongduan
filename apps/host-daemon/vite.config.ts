import { defineConfig } from "vite-plus";

export default defineConfig({
  lint: {
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  pack: {
    deps: {
      alwaysBundle: [
        "@wterm/core",
        "@wterm/ghostty",
        "@zhongduan/protocol",
        "@zhongduan/telemetry",
      ],
      neverBundle: ["node-pty"],
      onlyBundle: ["zod"],
      onlyImport: ["node-pty"],
    },
    entry: ["src/index.ts", "src/cli.ts"],
    format: ["esm"],
    platform: "node",
    sourcemap: true,
  },
});
