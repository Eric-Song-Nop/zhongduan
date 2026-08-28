import { randomBytes } from "node:crypto";

import type { EventJournalOptions } from "./journal";
import { EventJournal } from "./journal";
import { FakeTerminalAuthority } from "./fake-terminal-authority";
import { spawnNodePty, type SpawnNodePtyOptions } from "./node-pty-process";
import { TerminalSession } from "./session";
import type { TerminalAuthority } from "./terminal-authority";

export interface LocalSessionOptions extends SpawnNodePtyOptions {
  authority?: TerminalAuthority;
  journal?: EventJournalOptions;
  sessionEpoch?: bigint;
}

export function startLocalSession(options: LocalSessionOptions): TerminalSession {
  return new TerminalSession({
    authority: options.authority ?? new FakeTerminalAuthority(),
    journal: new EventJournal(options.journal),
    pty: spawnNodePty(options),
    sessionEpoch: options.sessionEpoch ?? createSessionEpoch(),
  });
}

export function createSessionEpoch(): bigint {
  let epoch = 0n;
  while (epoch === 0n) epoch = randomBytes(8).readBigUInt64BE();
  return epoch;
}
