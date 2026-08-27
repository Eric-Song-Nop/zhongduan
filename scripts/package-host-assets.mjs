import { createHash } from "node:crypto";
import { copyFile, mkdir, readFile } from "node:fs/promises";

const sourceWasm = new URL(
  "../vendor/wterm/packages/@wterm/ghostty/wasm/ghostty-vt.wasm",
  import.meta.url,
);
const sourceManifest = new URL(
  "../vendor/wterm/packages/@wterm/ghostty/engine-manifest.json",
  import.meta.url,
);
const outputDirectory = new URL("../apps/host-daemon/dist/", import.meta.url);
const outputWasm = new URL("ghostty-vt.wasm", outputDirectory);
const outputManifest = new URL("ghostty-engine-manifest.json", outputDirectory);

const [wasm, manifestBytes] = await Promise.all([readFile(sourceWasm), readFile(sourceManifest)]);
const manifest = JSON.parse(manifestBytes.toString("utf8"));
const digest = createHash("sha256").update(wasm).digest("hex");
if (manifest.wasmSha256 !== digest || manifest.provenance?.committedWasmSha256 !== digest) {
  throw new Error("Pinned Ghostty manifest does not match its committed WASM");
}

await mkdir(outputDirectory, { recursive: true });
await Promise.all([copyFile(sourceWasm, outputWasm), copyFile(sourceManifest, outputManifest)]);

console.log(`packaged Ghostty ${manifest.engineId} (${digest})`);
