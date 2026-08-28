import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { createServer } from "node:http";
import { access, readFile, readdir, rm, stat } from "node:fs/promises";
import { extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const appRoot = fileURLToPath(new URL("../apps/terminal-cloud/", import.meta.url));
const submoduleRoot = fileURLToPath(new URL("../vendor/wterm/", import.meta.url));
const vp = fileURLToPath(new URL("../node_modules/vite-plus/bin/vp", import.meta.url));
const clientDist = fileURLToPath(new URL("../apps/terminal-cloud/dist/client/", import.meta.url));
const appDist = new URL("../apps/terminal-cloud/dist/", import.meta.url);
const engineManifest = new URL(
  "../vendor/wterm/packages/@wterm/ghostty/engine-manifest.json",
  import.meta.url,
);
const EXPECTED_ENGINE_ID =
  "ghostty:fe317f850c3ab212f6638122c459b9b48b99a016:wterm-engine-sha256:2836a71302115fb2989235666560fc4b6a5739158478e85e4d25bd1f1248fe96";
const EXPECTED_WASM_SHA256 = "6d817a66a606e88ec3c0cefa9ab26f042b285577095588b6569d62838718edc5";
const SUBMODULE_DISPLAY_PATH = "vendor/wterm";
const wtermDistDirectories = ["core", "ghostty", "dom"].map(
  (name) => new URL(`../vendor/wterm/packages/@wterm/${name}/dist`, import.meta.url),
);

try {
  await removeGeneratedOutputs();
  assertNoSourceJavaScript();
  run(process.execPath, ["scripts/prepare-wterm.mjs"]);
  run(process.execPath, [vp, "build"], false, appRoot);

  const assets = await readdir(join(clientDist, "assets"));
  const wasm = assets.find((entry) => /^ghostty-vt-.*\.wasm$/u.test(entry));
  const snapshotWorker = assets.find((entry) => /^snapshot\.worker-.*\.js$/u.test(entry));
  if (wasm === undefined || snapshotWorker === undefined) {
    throw new Error("Browser build is missing its Ghostty WASM or dedicated snapshot worker");
  }
  await Promise.all([
    access(join(clientDist, "index.html")),
    access(join(clientDist, "assets", wasm)),
    access(join(clientDist, "assets", snapshotWorker)),
  ]);

  const manifest = JSON.parse(await readFile(engineManifest, "utf8"));
  const wasmBytes = await readFile(join(clientDist, "assets", wasm));
  const wasmSha256 = createHash("sha256").update(wasmBytes).digest("hex");
  if (
    manifest.engineId !== EXPECTED_ENGINE_ID ||
    manifest.wasmSha256 !== EXPECTED_WASM_SHA256 ||
    manifest.provenance?.committedWasmSha256 !== EXPECTED_WASM_SHA256 ||
    wasmSha256 !== EXPECTED_WASM_SHA256
  ) {
    throw new Error("Browser Ghostty assets do not match the pinned engine manifest");
  }

  let bundledEngineId = false;
  for (const asset of assets.filter((entry) => entry.endsWith(".js"))) {
    const source = await readFile(join(clientDist, "assets", asset), "utf8");
    bundledEngineId ||= source.includes(EXPECTED_ENGINE_ID);
    if (
      /^(?:import|export)\s+(?:[^"']+\s+from\s+)?["']@wterm\//mu.test(source) ||
      /\bimport\s*\(\s*["']@wterm\//u.test(source)
    ) {
      throw new Error(`${asset} retains an external @wterm import`);
    }
  }
  if (!bundledEngineId) throw new Error("Browser bundle does not contain the pinned engine ID");

  await Promise.all(wtermDistDirectories.map(remove));
  await assertMissingWtermDist();
  await smokeStaticBundle(wasm, snapshotWorker);
  assertNoSourceJavaScript();
} finally {
  await Promise.all([...wtermDistDirectories.map(remove), remove(appDist)]);
}

await assertMissingWtermDist();
const status = run("git", ["status", "--porcelain", "--untracked-files=all"], true, submoduleRoot);
if (status.stdout.trim().length !== 0) {
  throw new Error(`Clean browser gate left the pinned submodule dirty:\n${status.stdout}`);
}
assertNoSourceJavaScript();
console.log("clean browser build and bundled asset startup smoke passed");

async function smokeStaticBundle(wasm, snapshotWorker) {
  const server = createServer(async (request, response) => {
    try {
      const requestPath = new URL(request.url ?? "/", "http://127.0.0.1").pathname;
      const relative = requestPath === "/" ? "index.html" : requestPath.slice(1);
      const normalized = normalize(relative);
      const candidate = normalized.startsWith("..") ? "index.html" : normalized;
      let file = join(clientDist, candidate);
      try {
        await stat(file);
      } catch {
        file = join(clientDist, "index.html");
      }
      const body = await readFile(file);
      const contentType =
        extname(file) === ".wasm"
          ? "application/wasm"
          : extname(file) === ".js"
            ? "text/javascript"
            : extname(file) === ".css"
              ? "text/css"
              : "text/html";
      response.writeHead(200, { "Content-Type": contentType });
      response.end(body);
    } catch {
      response.writeHead(500);
      response.end();
    }
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    const address = server.address();
    if (address === null || typeof address === "string")
      throw new Error("static smoke did not bind");
    const base = `http://127.0.0.1:${address.port}`;
    for (const path of [
      "/sessions/session_clean_0001",
      `/assets/${wasm}`,
      `/assets/${snapshotWorker}`,
    ]) {
      const response = await fetch(`${base}${path}`);
      if (!response.ok || (await response.arrayBuffer()).byteLength === 0) {
        throw new Error(`Bundled browser asset did not start at ${path}`);
      }
    }
  } finally {
    await new Promise((resolve, reject) =>
      server.close((error) => (error === undefined ? resolve() : reject(error))),
    );
  }
}

async function removeGeneratedOutputs() {
  await Promise.all([...wtermDistDirectories.map(remove), remove(appDist)]);
  await assertMissingWtermDist();
}

async function remove(target) {
  await rm(target, { force: true, recursive: true });
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

function assertNoSourceJavaScript() {
  const rootUntracked = run(
    "git",
    ["ls-files", "--others", "--exclude-standard", "--", "apps", "packages", "vite.config.js"],
    true,
  )
    .stdout.split("\n")
    .filter(Boolean);
  const generated = rootUntracked.filter(
    (path) =>
      path === "vite.config.js" || (/^(?:apps|packages)\//u.test(path) && path.endsWith(".js")),
  );
  const wtermGenerated = run(
    "git",
    ["ls-files", "--others", "--exclude-standard", "--", "*.js"],
    true,
    submoduleRoot,
  )
    .stdout.split("\n")
    .filter(Boolean)
    .map((path) => `${SUBMODULE_DISPLAY_PATH}/${path}`);
  generated.push(...wtermGenerated);
  if (generated.length > 0) {
    throw new Error(`TypeScript emitted JavaScript beside source:\n${generated.join("\n")}`);
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
