import { readSocketAttachment, type RelayChannel, type SocketAttachment } from "./relay-socket";
import type { TicketRow } from "./relay-store";

export type ConnectionPhase =
  | "reserved"
  | "control-open"
  | "data-open"
  | "paired"
  | "ready"
  | "closed";

export interface ConnectionSockets {
  control: WebSocket | undefined;
  data: WebSocket | undefined;
  phase: ConnectionPhase;
}

export function deriveConnectionPhase(
  controlState: SocketAttachment["controlState"] | undefined,
  dataOpen: boolean,
  reserved: boolean,
): ConnectionPhase {
  const controlOpen = controlState !== undefined;
  if (controlOpen && dataOpen) return controlState === "active" ? "ready" : "paired";
  if (controlOpen) return "control-open";
  if (dataOpen) return "data-open";
  return reserved ? "reserved" : "closed";
}

export function sameConnection(candidate: SocketAttachment, identity: SocketAttachment): boolean {
  if (
    candidate.peer !== identity.peer ||
    candidate.connectionSetId !== identity.connectionSetId ||
    candidate.connectionId !== identity.connectionId
  ) {
    return false;
  }
  if (identity.peer === "host") {
    return candidate.hostFence !== null && candidate.hostFence === identity.hostFence;
  }
  return (
    candidate.clientId !== null &&
    candidate.clientId === identity.clientId &&
    candidate.deliveryGeneration === identity.deliveryGeneration
  );
}

export function connectionSockets(
  state: DurableObjectState,
  identity: SocketAttachment,
  reserved = false,
  except?: WebSocket,
): ConnectionSockets {
  let control: WebSocket | undefined;
  let data: WebSocket | undefined;
  for (const socket of state.getWebSockets(`set:${identity.connectionSetId}`)) {
    if (socket === except || socket.readyState !== WebSocket.OPEN) continue;
    const attachment = readSocketAttachment(socket);
    if (attachment === undefined || !sameConnection(attachment, identity)) continue;
    if (attachment.channel === "control") control = socket;
    else data = socket;
  }

  const phase = deriveConnectionPhase(
    control === undefined ? undefined : readSocketAttachment(control)?.controlState,
    data !== undefined,
    reserved,
  );
  return { control, data, phase };
}

export function matchingSocket(
  state: DurableObjectState,
  identity: SocketAttachment,
  channel: RelayChannel,
  except?: WebSocket,
): WebSocket | undefined {
  return connectionSockets(state, identity, false, except)[channel];
}

export function eligibleControlForDataTicket(
  state: DurableObjectState,
  ticket: TicketRow,
  currentHostFence: string | undefined,
): SocketAttachment | undefined {
  if (ticket.channel !== "data") return undefined;
  for (const socket of state.getWebSockets(`set:${ticket.connection_set_id}`)) {
    if (socket.readyState !== WebSocket.OPEN) continue;
    const attachment = readSocketAttachment(socket);
    if (
      attachment?.channel !== "control" ||
      attachment.peer !== ticket.peer ||
      attachment.connectionSetId !== ticket.connection_set_id ||
      attachment.connectionId !== ticket.connection_id
    ) {
      continue;
    }
    if (ticket.peer === "host") {
      return attachment.hostFence !== null && attachment.hostFence === currentHostFence
        ? attachment
        : undefined;
    }
    return ticket.client_id !== null && attachment.clientId === ticket.client_id
      ? attachment
      : undefined;
  }
  return undefined;
}
