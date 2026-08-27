import { defineConfig } from "vite-plus";

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
  run: {
    tasks: {
      verify: {
        command: ["vp check", "vp test --run"],
        cache: false,
      },
    },
  },
});
