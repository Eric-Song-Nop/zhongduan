import { execFileSync } from "node:child_process";
import { access } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const trackedSources = execFileSync(
  "git",
  ["ls-files", "--cached", "--others", "--exclude-standard", "-z", "--", "*.ts", "*.tsx"],
  { cwd: root, encoding: "utf8" },
)
  .split("\0")
  .filter(Boolean);
const emitted = [];
for (const source of trackedSources) {
  const javascript = source.replace(/\.tsx?$/u, ".js");
  try {
    await access(resolve(root, javascript));
    emitted.push(javascript);
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
}
if (emitted.length > 0) {
  throw new Error(`TypeScript emitted JavaScript beside source files:\n${emitted.join("\n")}`);
}
console.log("no JavaScript emitted beside TypeScript sources");
