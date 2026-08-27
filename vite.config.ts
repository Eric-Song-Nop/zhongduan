import { defineConfig } from "vite-plus";
import { configDefaults } from "vite-plus/test/config";

export default defineConfig({
  fmt: {
    ignorePatterns: ["resource.md", "snapshot-police.md"],
    sortPackageJson: true,
  },
  lint: {
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  test: {
    exclude: [...configDefaults.exclude, "apps/terminal-cloud/test/**/*.test.ts"],
  },
  run: {
    tasks: {
      verify: {
        command: ["vp check", "vp test --run", "vp run @zhongduan/terminal-cloud#test"],
        cache: false,
      },
    },
  },
});
