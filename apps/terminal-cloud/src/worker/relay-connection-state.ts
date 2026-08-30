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
  controlReady: boolean | undefined,
  dataOpen: boolean,
  reserved: boolean,
): ConnectionPhase {
  const controlOpen = controlReady !== undefined;
  if (controlOpen && dataOpen) return controlReady ? "ready" : "paired";
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
    control === undefined ? undefined : readSocketAttachment(control)?.ready,
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
  currentReadyHostFence?: string,
): SocketAttachment | undefined {
  if (ticket.channel !== "data") return undefined;
  for (const socket of state.getWebSockets(`set:${ticket.connection_set_id}`)) {
    if (socket.readyState !== WebSocket.OPEN) continue;
    const attachment = readSocketAttachment(socket);
    if (
      attachment?.channel !== "control" ||
      attachment.peer !== ticket.peer ||
      attachment.connectionSetId !== ticket.connection_set_id ||
      attachment.connectionId !== ticket.connection_id ||
      attachment.subject !== ticket.subject ||
      attachment.role !== ticket.role ||
      attachment.streamId !== ticket.stream_id ||
      attachment.deliveryGeneration !== ticket.delivery_generation
    ) {
      continue;
    }
    if (ticket.peer === "host") {
      return attachment.hostFence !== null && attachment.hostFence === currentHostFence
        ? attachment
        : undefined;
    }
    if (ticket.client_id === null || attachment.clientId !== ticket.client_id) {
      return undefined;
    }
    return attachment.hostFence === null &&
      ticket.host_fence !== null &&
      ticket.host_fence === currentReadyHostFence
      ? attachment
      : undefined;
  }
  return undefined;
}
