import { defineConfig } from "vite-plus";
import { configDefaults } from "vite-plus/test/config";

export default defineConfig({
  fmt: {
    ignorePatterns: ["resource.md", "snapshot-police.md", "vendor/wterm/**"],
    sortPackageJson: true,
  },
  lint: {
    ignorePatterns: ["vendor/wterm/**"],
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  test: {
    exclude: [
      ...configDefaults.exclude,
      "apps/terminal-cloud/test/**/*.test.ts",
      "vendor/wterm/**",
    ],
  },
  run: {
    tasks: {
      build: {
        command: "vp run @zhongduan/host-daemon#build",
        cache: false,
      },
      "build-browser": {
        command: "vp run @zhongduan/terminal-cloud#build",
        cache: false,
      },
      verify: {
        command: [
          "node scripts/prepare-wterm.mjs",
          "vp check",
          "node scripts/verify-no-source-emit.mjs",
          "vp test --run",
          "vp run @zhongduan/terminal-cloud#test",
          "python3 -B -m unittest scripts/test_browser_e2e_contract.py",
          "vp run @zhongduan/terminal-cloud#test:browser",
        ],
        cache: false,
      },
      "verify-clean-host": {
        command: "node scripts/verify-clean-host-build.mjs",
        cache: false,
      },
      "verify-no-source-emit": {
        command: "node scripts/verify-no-source-emit.mjs",
        cache: false,
      },
      "verify-clean-browser": {
        command: "node scripts/verify-clean-browser-build.mjs",
        cache: false,
      },
      "verify-browser-recovery-smoke": {
        command: "python3 -B scripts/verify-browser-e2e.py",
        cache: false,
      },
      "verify-browser-recovery-smoke-contract": {
        command: "python3 -B -m unittest scripts/test_browser_e2e_contract.py",
        cache: false,
      },
    },
  },
});
