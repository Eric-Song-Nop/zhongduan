import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const EXPECTED_SHA = "a00eb78f7698dcb998c8695d4c84a3d462a198b2";
const EXPECTED_URL = "https://github.com/Eric-Song-Nop/wterm.git";
const SUBMODULE_PATH = "vendor/wterm";
const root = fileURLToPath(new URL("..", import.meta.url));

function git(args, cwd = root) {
  return execFileSync("git", args, { cwd, encoding: "utf8" }).trimEnd();
}

function assertEqual(actual, expected, label) {
  if (actual !== expected) {
    throw new Error(`${label}: expected ${expected}, received ${actual || "<empty>"}`);
  }
}

assertEqual(
  git(["config", "-f", ".gitmodules", "--get", `submodule.${SUBMODULE_PATH}.url`]),
  EXPECTED_URL,
  "wterm .gitmodules URL",
);

const gitlink = git(["ls-files", "--stage", "--", SUBMODULE_PATH]).match(
  /^160000 ([0-9a-f]{40}) 0\tvendor\/wterm$/,
);
if (gitlink === null) throw new Error("vendor/wterm is not a Git submodule");
assertEqual(gitlink[1], EXPECTED_SHA, "wterm gitlink SHA");

const submoduleRoot = fileURLToPath(new URL(`../${SUBMODULE_PATH}/`, import.meta.url));
assertEqual(git(["rev-parse", "HEAD"], submoduleRoot), EXPECTED_SHA, "wterm checkout SHA");
assertEqual(git(["remote", "get-url", "origin"], submoduleRoot), EXPECTED_URL, "wterm origin URL");
assertEqual(git(["status", "--porcelain"], submoduleRoot), "", "wterm worktree status");
