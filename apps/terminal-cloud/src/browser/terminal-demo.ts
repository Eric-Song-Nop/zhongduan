import { GhosttyCore } from "@wterm/ghostty";
import type { ReplicaCursor } from "@zhongduan/protocol";
import type { SnapshotManifest } from "@zhongduan/session-client";

import type { WTermReplicaHost } from "./replica-host";

declare global {
  interface Window {
    __zhongduanE2E?: {
      adopt(): Promise<void>;
      engineId: string;
      write(text: string): void;
    };
  }
}

const encoder = new TextEncoder();

export async function installTerminalDemo(host: WTermReplicaHost): Promise<void> {
  host.active?.writePty(
    encoder.encode(
      "\x1b[2J\x1b[H\x1b[38;5;81mzhongduan\x1b[0m  remote shell\r\n" +
        "\x1b[38;5;244m────────────────────────────────────────\x1b[0m\r\n" +
        "\x1b[38;5;114m● connected\x1b[0m  ghostty session ready\r\n\r\n" +
        "demo@remote ~/project $ \x1b[38;5;214mpnpm test\x1b[0m\r\n" +
        "✓ protocol  126 tests\r\n✓ terminal  42 tests\r\n\r\n" +
        "demo@remote ~/project $ ",
    ),
  );

  let adopting: Promise<void> | null = null;
  window.__zhongduanE2E = {
    adopt: () => {
      adopting ??= adoptDemoSnapshot(host).finally(() => {
        adopting = null;
      });
      return adopting;
    },
    engineId: host.engineId,
    write: (text) => host.active?.writePty(encoder.encode(text)),
  };
}

async function adoptDemoSnapshot(host: WTermReplicaHost): Promise<void> {
  const authority = await GhosttyCore.load({
    effects: "discard",
    foregroundColor: "#d8dee9",
    backgroundColor: "#111315",
  });
  authority.init(80, 24);
  authority.writeString(
    "\x1b[2J\x1b[H\x1b[38;5;81mzhongduan\x1b[0m  restored checkpoint\r\n" +
      "\x1b[38;5;244m────────────────────────────────────────\x1b[0m\r\n" +
      "\x1b[38;5;114m● replay committed\x1b[0m\r\n\r\n" +
      "demo@remote ~/project $ ",
  );
  const snapshot = authority.encodeSnapshot();
  authority.dispose();
  const manifest: SnapshotManifest = {
    type: "snapshot-manifest",
    snapshotId: "snapshot_demo_0001",
    engineId: host.engineId,
    sessionEpoch: "1",
    streamId: 1,
    deliveryGeneration: "1",
    cutEventSeq: "1",
    nextPtyOffset: "1",
    commitEventSeq: "2",
    commitPtyOffset: "2",
    compression: "none",
    compressedLength: snapshot.byteLength.toString(),
    uncompressedLength: snapshot.byteLength.toString(),
    sha256: "a".repeat(64),
    downloadPath: "/api/v1/sessions/session_demo_0001/snapshots/snapshot_demo_0001",
    restoreThrough: "finish",
  };
  const abort = new AbortController();
  const candidate = await host.restore(snapshot, manifest, abort.signal);
  candidate.writePty(encoder.encode("\x1b[38;5;214mecho adopted\x1b[0m\r\nadopted\r\n"));
  const cursor: ReplicaCursor = {
    sessionEpoch: 1n,
    deliveryGeneration: 1n,
    lastEventSeq: 2n,
    nextPtyOffset: 2n,
  };
  host.adopt(candidate, cursor);
}
