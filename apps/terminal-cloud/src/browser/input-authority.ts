import type { InputIdentity } from "./input-intent-ledger";
import type { InputFrame } from "./input-codec";

export type InputTransportSendResult = "accepted" | "proven-not-accepted" | "uncertain";

export interface InputTransport {
  readonly generation: number;
  readonly sender: (frame: InputFrame) => InputTransportSendResult;
  readonly writerFence?: string;
  readonly writerLease?: string;
}

type WritableInputTransport = InputTransport & {
  readonly writerFence: string;
  readonly writerLease: string;
};

export interface InputEpochState {
  readonly writerFence: string;
  readonly inputEpoch: string;
  readonly nextSequence: bigint;
}

interface AuthorityBase {
  readonly highestWriterFence: bigint;
}

export type AuthorityState =
  | (AuthorityBase & {
      readonly state: "detached";
    })
  | (AuthorityBase & {
      readonly state: "read-only";
      readonly transport: InputTransport;
      readonly reason: "missing-authority" | "stale-fence";
    })
  | (AuthorityBase & {
      readonly state: "open";
      readonly transport: WritableInputTransport;
      readonly epoch: InputEpochState;
    })
  | (AuthorityBase & {
      readonly state: "sealed";
      readonly transport: InputTransport;
      readonly epoch: InputEpochState;
      readonly reason: "authority-revoked" | "replacement-required" | "transport-replaced";
    });

export function initialAuthorityState(): AuthorityState {
  return { state: "detached", highestWriterFence: 0n };
}

export function positiveWriterFence(value: string | undefined): bigint | null {
  if (value === undefined) return null;
  try {
    const parsed = BigInt(value);
    return parsed > 0n && parsed.toString() === value ? parsed : null;
  } catch {
    return null;
  }
}

export function authorityTransport(state: AuthorityState): InputTransport | null {
  return state.state === "detached" ? null : state.transport;
}

export function authorityEpoch(state: AuthorityState): InputEpochState | null {
  return state.state === "open" || state.state === "sealed" ? state.epoch : null;
}

export function controlReplacementRequired(state: AuthorityState): boolean {
  return state.state === "sealed" && state.reason === "replacement-required";
}

export function isWritableAuthority(
  state: AuthorityState,
): state is Extract<AuthorityState, { state: "open" }> {
  return state.state === "open";
}

export function attachAuthority(
  current: AuthorityState,
  transport: InputTransport,
  inputEpoch: string,
): { readonly authority: AuthorityState; readonly accepted: boolean } {
  const fence = positiveWriterFence(transport.writerFence);
  if (fence !== null && transport.writerLease !== undefined && fence > current.highestWriterFence) {
    return {
      accepted: true,
      authority: {
        state: "open",
        highestWriterFence: fence,
        transport: transport as WritableInputTransport,
        epoch: {
          writerFence: transport.writerFence!,
          inputEpoch,
          nextSequence: 1n,
        },
      },
    };
  }

  // A repeated call with the exact current transport is an idempotent failed attachment, not a
  // replacement event. Preserve the already-open epoch just as the monolithic E1 owner did.
  if (current.state === "open" && current.transport === transport) {
    return { accepted: false, authority: current };
  }

  // An invalid replacement cannot erase the epoch or downgrade why it was sealed. In particular,
  // replacement-required remains derived from this union member until a higher fence is accepted.
  if (current.state === "sealed") {
    return {
      accepted: false,
      authority: { ...current, transport },
    };
  }

  return {
    accepted: false,
    authority: {
      state: "read-only",
      highestWriterFence: current.highestWriterFence,
      transport,
      reason:
        fence === null || transport.writerLease === undefined ? "missing-authority" : "stale-fence",
    },
  };
}

export function detachAuthority(current: AuthorityState): AuthorityState {
  return { state: "detached", highestWriterFence: current.highestWriterFence };
}

export function revokeAuthority(current: AuthorityState): AuthorityState {
  if (current.state === "detached") return current;
  const transport: InputTransport = {
    generation: current.transport.generation,
    sender: current.transport.sender,
  };
  if (current.state === "open") {
    return {
      state: "sealed",
      highestWriterFence: current.highestWriterFence,
      transport,
      epoch: current.epoch,
      reason: "authority-revoked",
    };
  }
  if (current.state === "sealed") return { ...current, transport };
  return { ...current, transport, reason: "missing-authority" };
}

export function sealAuthority(
  current: AuthorityState,
  reason: Extract<AuthorityState, { state: "sealed" }>["reason"],
): AuthorityState {
  if (current.state === "open") {
    return {
      state: "sealed",
      highestWriterFence: current.highestWriterFence,
      transport: current.transport,
      epoch: current.epoch,
      reason,
    };
  }
  if (current.state !== "sealed") return current;
  if (current.reason === "replacement-required" || current.reason === reason) return current;
  return {
    ...current,
    reason: reason === "replacement-required" ? reason : current.reason,
  };
}

export function identityBelongsToAuthority(
  state: AuthorityState,
  identity: InputIdentity,
): boolean {
  const epoch = authorityEpoch(state);
  return (
    epoch !== null &&
    epoch.writerFence === identity.writerFence &&
    epoch.inputEpoch === identity.inputEpoch
  );
}

export function allocateInputIdentity(current: Extract<AuthorityState, { state: "open" }>): {
  readonly authority: Extract<AuthorityState, { state: "open" }>;
  readonly identity: InputIdentity;
} {
  const identity = Object.freeze({
    writerFence: current.epoch.writerFence,
    inputEpoch: current.epoch.inputEpoch,
    clientInputSeq: current.epoch.nextSequence.toString(),
  });
  return {
    identity,
    authority: {
      ...current,
      epoch: { ...current.epoch, nextSequence: current.epoch.nextSequence + 1n },
    },
  };
}

export function assertAuthorityInvariants(state: AuthorityState): void {
  if (state.highestWriterFence < 0n) throw new Error("authority highest fence cannot be negative");
  if (state.state === "detached") return;
  if (state.state === "open") {
    const fence = positiveWriterFence(state.transport.writerFence);
    if (
      fence === null ||
      state.transport.writerLease.length === 0 ||
      fence !== state.highestWriterFence ||
      state.epoch.writerFence !== state.transport.writerFence ||
      state.epoch.nextSequence < 1n
    ) {
      throw new Error("open authority contains an inconsistent writer/epoch pair");
    }
    return;
  }
  if (state.state === "sealed") {
    const epochFence = positiveWriterFence(state.epoch.writerFence);
    if (
      epochFence === null ||
      epochFence > state.highestWriterFence ||
      state.epoch.nextSequence < 1n
    ) {
      throw new Error("sealed authority contains an inconsistent epoch");
    }
  }
}
