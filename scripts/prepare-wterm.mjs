import { execFileSync } from "node:child_process";
import { access, rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const submoduleRoot = fileURLToPath(new URL("../vendor/wterm/", import.meta.url));
const vp = fileURLToPath(new URL("../node_modules/vite-plus/bin/vp", import.meta.url));
const distDirectories = [
  "vendor/wterm/packages/@wterm/core/dist",
  "vendor/wterm/packages/@wterm/ghostty/dist",
  "vendor/wterm/packages/@wterm/dom/dist",
];
const requiredOutputs = [
  "vendor/wterm/packages/@wterm/core/dist/index.js",
  "vendor/wterm/packages/@wterm/core/dist/index.d.ts",
  "vendor/wterm/packages/@wterm/ghostty/dist/index.js",
  "vendor/wterm/packages/@wterm/ghostty/dist/index.d.ts",
  "vendor/wterm/packages/@wterm/dom/dist/index.js",
  "vendor/wterm/packages/@wterm/dom/dist/index.d.ts",
];

execFileSync(process.execPath, ["scripts/verify-wterm-submodule.mjs"], {
  cwd: root,
  stdio: "inherit",
});

await Promise.all(
  distDirectories.map((directory) =>
    rm(new URL(`../${directory}`, import.meta.url), {
      force: true,
      recursive: true,
    }),
  ),
);

for (const target of ["@wterm/core#build", "@wterm/ghostty#build", "@wterm/dom#build"]) {
  execFileSync(process.execPath, [vp, "run", "--no-cache", "-t", target], {
    cwd: root,
    stdio: "inherit",
  });
}

await Promise.all(
  requiredOutputs.map((output) => access(new URL(`../${output}`, import.meta.url))),
);

const status = execFileSync("git", ["status", "--porcelain", "--untracked-files=all"], {
  cwd: submoduleRoot,
  encoding: "utf8",
}).trim();
if (status.length !== 0) {
  throw new Error(`Wterm build modified the pinned submodule:\n${status}`);
}
