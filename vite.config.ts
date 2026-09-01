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
          "node scripts/e0-terminal-journey.ts",
          "node scripts/e0_authority_oracle.mjs",
          "vp check",
          "node scripts/verify-no-source-emit.mjs",
          "vp test --run",
          "vp run @zhongduan/terminal-cloud#test",
          "vp run @zhongduan/terminal-cloud#test:browser",
        ],
        cache: false,
      },
      "verify-phase0": {
        command: [
          "vp test --run packages/protocol/src/control-frame.test.ts packages/protocol/src/cloud-api.test.ts apps/host-daemon/src/cloud/delivery-recovery-queue.test.ts apps/host-daemon/src/cloud/delivery-barrier-waiter.test.ts apps/host-daemon/src/cloud/delivery-scheduler.test.ts",
          "pnpm --dir apps/terminal-cloud exec vp test --config vitest.worker.config.ts --run test/relay.test.ts test/runtime.test.ts",
          "pnpm --dir apps/terminal-cloud exec vp test --config vitest.browser.config.ts --run src/browser/terminal-session.test.ts",
        ],
        cache: false,
      },
      "verify-e0-contract": {
        command: [
          "node scripts/prepare-wterm.mjs",
          "node scripts/e0-terminal-journey.ts",
          "vp test --run scripts/e0-terminal-journey.test.ts",
          "node scripts/e0_authority_oracle.mjs",
          "node scripts/verify-e0-terminal-journey.ts --matrix-plan",
        ],
        cache: false,
      },
      "verify-e0-local": {
        command: "node scripts/verify-e0-terminal-journey.ts",
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
    },
  },
});
