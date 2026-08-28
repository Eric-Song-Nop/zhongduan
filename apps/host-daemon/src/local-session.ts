import { randomBytes } from "node:crypto";

import { GHOSTTY_TERMINAL_PROFILE } from "@wterm/ghostty";

import type { EventJournalOptions } from "./journal";
import { EventJournal } from "./journal";
import { spawnNodePty, type SpawnNodePtyOptions } from "./node-pty-process";
import { TerminalSession } from "./session";
import type { TerminalAuthority } from "./terminal-authority";
import { WtermGhosttyAuthority } from "./wterm-ghostty-authority";

export interface LocalSessionOptions extends Omit<SpawnNodePtyOptions, "name"> {
  authority?: TerminalAuthority;
  heightPx?: number;
  journal?: EventJournalOptions;
  scrollbackLimit?: number;
  sessionEpoch?: bigint;
  widthPx?: number;
}

export async function startLocalSession(options: LocalSessionOptions): Promise<TerminalSession> {
  const journal = new EventJournal(options.journal);
  const authority =
    options.authority ??
    (await WtermGhosttyAuthority.create({
      cols: options.cols,
      heightPx: options.heightPx ?? 0,
      rows: options.rows,
      widthPx: options.widthPx ?? 0,
      ...(options.scrollbackLimit === undefined
        ? {}
        : { scrollbackLimit: options.scrollbackLimit }),
    }));

  let pty;
  try {
    pty = spawnNodePty({
      cols: options.cols,
      command: options.command,
      env: {
        ...(options.env ?? process.env),
        TERM: GHOSTTY_TERMINAL_PROFILE.term,
      },
      name: GHOSTTY_TERMINAL_PROFILE.term,
      rows: options.rows,
      ...(options.args === undefined ? {} : { args: options.args }),
      ...(options.cwd === undefined ? {} : { cwd: options.cwd }),
    });
  } catch (error) {
    bestEffort(() => authority.dispose());
    throw error;
  }

  try {
    return new TerminalSession({
      authority,
      journal,
      pty,
      sessionEpoch: options.sessionEpoch ?? createSessionEpoch(),
    });
  } catch (error) {
    bestEffort(() => authority.dispose());
    bestEffort(() => pty.kill());
    throw error;
  }
}

export function createSessionEpoch(): bigint {
  let epoch = 0n;
  while (epoch === 0n) epoch = randomBytes(8).readBigUInt64BE();
  return epoch;
}

function bestEffort(action: () => void): void {
  try {
    action();
  } catch {
    // Preserve the session creation failure while releasing independent owners.
  }
}
