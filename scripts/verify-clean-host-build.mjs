import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readdir, readFile, rm, stat } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const submoduleRoot = fileURLToPath(new URL("../vendor/wterm/", import.meta.url));
const vp = fileURLToPath(new URL("../node_modules/vite-plus/bin/vp", import.meta.url));
const wtermDistDirectories = [
  new URL("../vendor/wterm/packages/@wterm/core/dist", import.meta.url),
  new URL("../vendor/wterm/packages/@wterm/ghostty/dist", import.meta.url),
  new URL("../vendor/wterm/packages/@wterm/dom/dist", import.meta.url),
];
const hostDist = new URL("../apps/host-daemon/dist/", import.meta.url);
const packagedWasm = new URL("ghostty-vt.wasm", hostDist);
const packagedManifest = new URL("ghostty-engine-manifest.json", hostDist);
const cli = fileURLToPath(new URL("cli.mjs", hostDist));

try {
  await removeGeneratedOutputs();
  run(process.execPath, [vp, "cache", "clean"]);
  run(process.execPath, [vp, "run", "--no-cache", "build"]);

  const [wasm, manifestBytes] = await Promise.all([
    readFile(packagedWasm),
    readFile(packagedManifest),
  ]);
  const manifest = JSON.parse(manifestBytes.toString("utf8"));
  const digest = createHash("sha256").update(wasm).digest("hex");
  if (manifest.wasmSha256 !== digest || manifest.provenance?.committedWasmSha256 !== digest) {
    throw new Error("Packaged Ghostty WASM does not match its packaged engine manifest");
  }

  const modules = (await readdir(hostDist)).filter((entry) => entry.endsWith(".mjs"));
  for (const module of modules) {
    const source = await readFile(new URL(module, hostDist), "utf8");
    if (
      /^(?:import|export)\s+(?:[^"']+\s+from\s+)?["']@wterm\//mu.test(source) ||
      /\bimport\s*\(\s*["']@wterm\//u.test(source)
    ) {
      throw new Error(`${module} retains an external @wterm import`);
    }
  }

  await Promise.all(
    wtermDistDirectories.map((directory) =>
      rm(directory, {
        force: true,
        recursive: true,
      }),
    ),
  );
  await assertMissingWtermDist();

  const smoke = run(
    process.execPath,
    [cli, "local", "--", "/bin/sh", "-c", 'printf "__CLEAN_PACKED_GHOSTTY__:%s\\n" "$TERM"'],
    true,
  );
  if (!smoke.stdout.includes("__CLEAN_PACKED_GHOSTTY__:xterm-256color")) {
    throw new Error(`Packed Host smoke returned unexpected output:\n${smoke.stdout}`);
  }
} finally {
  await Promise.all(
    wtermDistDirectories.map((directory) =>
      rm(directory, {
        force: true,
        recursive: true,
      }),
    ),
  );
}

await assertMissingWtermDist();
const status = run("git", ["status", "--porcelain", "--untracked-files=all"], true, submoduleRoot);
if (status.stdout.trim().length !== 0) {
  throw new Error(`Clean Host gate left the pinned submodule dirty:\n${status.stdout}`);
}

console.log("clean Host build and packed Ghostty smoke passed");

async function removeGeneratedOutputs() {
  await Promise.all([
    ...wtermDistDirectories.map((directory) => rm(directory, { force: true, recursive: true })),
    rm(hostDist, { force: true, recursive: true }),
  ]);
  await assertMissingWtermDist();
}

async function assertMissingWtermDist() {
  for (const directory of wtermDistDirectories) {
    try {
      await stat(directory);
      throw new Error(`Expected ${fileURLToPath(directory)} to be absent`);
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
    }
  }
}

function run(command, args, capture = false, cwd = root) {
  const result = spawnSync(command, args, {
    cwd,
    encoding: "utf8",
    stdio: capture ? "pipe" : "inherit",
  });
  if (result.error !== undefined) throw result.error;
  if (result.status !== 0) {
    throw new Error(
      `${command} ${args.join(" ")} exited ${result.status}\n${result.stdout ?? ""}${result.stderr ?? ""}`,
    );
  }
  return { stdout: result.stdout ?? "", stderr: result.stderr ?? "" };
}
