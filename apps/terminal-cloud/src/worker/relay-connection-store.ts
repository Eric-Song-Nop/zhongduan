import { MAX_U64 } from "@zhongduan/protocol";
import type { CapabilityRole } from "./auth";
import { RelayStore, type ClientRow, type TicketRow } from "./relay-store";

export interface ConnectionSetReservation {
  clientId: string | null;
  connectionId: string;
  connectionSetId: string;
  controlTicketDigest: string;
  dataTicketDigest: string;
  expiresAt: number;
  now: number;
  peer: "host" | "browser";
  principalIdHash: string | undefined;
  relayCapabilitiesJson: string;
  role: CapabilityRole;
  subject: string;
}

export interface BrowserDeliveryReplacement {
  clientId: string;
  connectionId: string;
  connectionSetId: string;
  currentGeneration: string;
  expiresAt: number;
  nextGeneration: string;
  relayCapabilitiesJson: string;
  role: CapabilityRole;
  streamId: number;
  subject: string;
  ticketDigest: string;
}

export type ConnectionSetReservationResult =
  | { client: ClientRow | undefined; deliveryGeneration: string; ok: true }
  | {
      ok: false;
      reason: "client-owner-mismatch" | "too-many-clients" | "too-many-pending-connections";
    };

type ClientReservationResult =
  | { client: ClientRow; ok: true }
  | { ok: false; reason: "client-owner-mismatch" | "too-many-clients" };

export class RelayConnectionStore {
  constructor(
    private readonly state: DurableObjectState,
    private readonly sql: SqlStorage,
    private readonly store: RelayStore,
    private readonly maxBrowserClients: number,
    private readonly maxPendingSets: number,
  ) {}

  reserveConnectionSet(input: ConnectionSetReservation): ConnectionSetReservationResult {
    let result: ConnectionSetReservationResult | undefined;
    this.state.storage.transactionSync(() => {
      this.#cleanupExpired(input.now);
      if (
        input.peer === "browser" &&
        input.clientId !== null &&
        input.principalIdHash !== undefined &&
        input.role !== "host"
      ) {
        const existing = this.store.clientById(input.clientId);
        if (
          existing !== undefined &&
          (existing.principal_id_hash !== input.principalIdHash || existing.role !== input.role)
        ) {
          result = { ok: false, reason: "client-owner-mismatch" };
          return;
        }
        this.#revokePending("browser", input.clientId, input.subject);
      } else {
        this.#revokePending("host", null, input.subject);
      }

      const pendingSetCount = this.sql
        .exec("SELECT COUNT(DISTINCT connection_set_id) AS value FROM connection_ticket")
        .one() as { value: number };
      if (pendingSetCount.value >= this.maxPendingSets) {
        result = { ok: false, reason: "too-many-pending-connections" };
        return;
      }

      let client: ClientRow | undefined;
      if (
        input.peer === "browser" &&
        input.clientId !== null &&
        input.principalIdHash !== undefined &&
        input.role !== "host"
      ) {
        const reservation = this.#reserveClient(
          input.clientId,
          input.principalIdHash,
          input.role,
          input.expiresAt,
          input.now,
        );
        if (!reservation.ok) {
          result = reservation;
          return;
        }
        client = reservation.client;
      }

      const streamId = client?.stream_id ?? 0;
      const deliveryGeneration = this.#reservedDeliveryGeneration(client);
      for (const [ticketDigest, channel] of [
        [input.controlTicketDigest, "control"],
        [input.dataTicketDigest, "data"],
      ] as const) {
        this.sql.exec(
          `INSERT INTO connection_ticket
            (ticket_digest, connection_set_id, connection_id, peer, channel,
             client_id, subject, role, stream_id, delivery_generation, expires_at,
             relay_capabilities_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          ticketDigest,
          input.connectionSetId,
          input.connectionId,
          input.peer,
          channel,
          input.clientId,
          input.subject,
          input.role,
          streamId,
          deliveryGeneration,
          input.expiresAt,
          input.relayCapabilitiesJson,
        );
      }
      result = { client, deliveryGeneration, ok: true };
    });
    if (result === undefined) throw new Error("connection reservation did not resolve");
    return result;
  }

  claimBrowserControl(ticket: TicketRow): TicketRow | undefined {
    if (ticket.client_id === null || ticket.channel !== "control") return undefined;
    let claimed: TicketRow | undefined;
    this.state.storage.transactionSync(() => {
      const now = Date.now();
      this.#cleanupExpired(now);
      const currentTicket = this.store.ticket(ticket.ticket_digest);
      const client = this.store.clientById(ticket.client_id!);
      const clientCount = this.sql.exec("SELECT COUNT(*) AS value FROM client_delivery").one() as {
        value: number;
      };
      if (
        currentTicket === undefined ||
        currentTicket.expires_at <= now ||
        currentTicket.connection_set_id !== ticket.connection_set_id ||
        client === undefined ||
        currentTicket.ticket_digest !== ticket.ticket_digest ||
        currentTicket.channel !== "control" ||
        currentTicket.client_id !== ticket.client_id ||
        currentTicket.role !== ticket.role ||
        currentTicket.stream_id !== ticket.stream_id ||
        currentTicket.delivery_generation !== ticket.delivery_generation ||
        client.role !== currentTicket.role ||
        client.stream_id !== currentTicket.stream_id ||
        clientCount.value > this.maxBrowserClients
      ) {
        return;
      }
      const currentGeneration = BigInt(client.delivery_generation);
      const reservedGeneration = BigInt(currentTicket.delivery_generation);
      const expectedGeneration =
        client.registered_at === null ? currentGeneration : currentGeneration + 1n;
      if (reservedGeneration !== expectedGeneration || reservedGeneration > MAX_U64) return;
      const registrationPredicate =
        client.registered_at === null ? "registered_at IS NULL" : "registered_at IS NOT NULL";
      this.sql.exec(
        `UPDATE client_delivery
         SET delivery_generation = ?, registered_at = COALESCE(registered_at, ?),
             reservation_expires_at = NULL, updated_at = ?
         WHERE client_id = ? AND delivery_generation = ? AND ${registrationPredicate}`,
        currentTicket.delivery_generation,
        now,
        now,
        ticket.client_id,
        client.delivery_generation,
      );
      const activatedClient = this.store.clientById(ticket.client_id!);
      if (
        activatedClient?.registered_at !== null &&
        activatedClient?.delivery_generation === currentTicket.delivery_generation
      ) {
        claimed = this.store.ticket(ticket.ticket_digest);
      }
    });
    return claimed;
  }

  discardReservation(ticket: TicketRow): void {
    this.state.storage.transactionSync(() => {
      const now = Date.now();
      this.sql.exec(
        "DELETE FROM connection_ticket WHERE connection_set_id = ?",
        ticket.connection_set_id,
      );
      if (ticket.client_id === null) return;
      this.sql.exec(
        `DELETE FROM client_delivery
         WHERE client_id = ? AND registered_at IS NULL
           AND NOT EXISTS (
             SELECT 1 FROM connection_ticket
             WHERE client_id = ? AND channel = 'control' AND expires_at > ?
           )`,
        ticket.client_id,
        ticket.client_id,
        now,
      );
    });
  }

  expireTicket(ticketDigest: string, now: number): void {
    this.state.storage.transactionSync(() => {
      this.sql.exec("DELETE FROM connection_ticket WHERE ticket_digest = ?", ticketDigest);
      this.#cleanupExpired(now);
    });
  }

  consumeTicket(ticketDigest: string): void {
    this.sql.exec("DELETE FROM connection_ticket WHERE ticket_digest = ?", ticketDigest);
  }

  closeConnectionSet(connectionSetId: string): void {
    this.sql.exec("DELETE FROM connection_ticket WHERE connection_set_id = ?", connectionSetId);
  }

  advanceHostFence(connectionSetId: string): string {
    let nextFence = "0";
    this.state.storage.transactionSync(() => {
      const session = this.store.session();
      if (session === undefined) throw new Error("session is not initialized");
      const currentFence = BigInt(session.host_fence);
      if (currentFence >= MAX_U64) throw new Error("host fence space exhausted");
      nextFence = (currentFence + 1n).toString();
      this.sql.exec(
        "UPDATE session_state SET host_fence = ?, updated_at = ? WHERE singleton = 1",
        nextFence,
        Date.now(),
      );
      this.sql.exec(
        "UPDATE connection_ticket SET host_fence = ? WHERE connection_set_id = ?",
        nextFence,
        connectionSetId,
      );
    });
    return nextFence;
  }

  invalidateHostConnection(connectionSetId: string, expectedFence: string): boolean {
    let invalidated = false;
    this.state.storage.transactionSync(() => {
      const session = this.store.session();
      if (session?.host_fence !== expectedFence) return;
      if (BigInt(expectedFence) >= MAX_U64) throw new Error("host fence space exhausted");
      this.sql.exec(
        "UPDATE session_state SET host_fence = ?, updated_at = ? WHERE singleton = 1",
        (BigInt(expectedFence) + 1n).toString(),
        Date.now(),
      );
      this.sql.exec("DELETE FROM connection_ticket WHERE connection_set_id = ?", connectionSetId);
      invalidated = true;
    });
    return invalidated;
  }

  replaceBrowserDelivery(input: BrowserDeliveryReplacement): boolean {
    let replaced = false;
    this.state.storage.transactionSync(() => {
      const client = this.store.clientById(input.clientId);
      if (
        client === undefined ||
        client.delivery_generation !== input.currentGeneration ||
        client.stream_id !== input.streamId ||
        client.role !== input.role
      ) {
        return;
      }
      this.sql.exec(
        `UPDATE client_delivery SET delivery_generation = ?, updated_at = ?
         WHERE client_id = ? AND delivery_generation = ?`,
        input.nextGeneration,
        Date.now(),
        input.clientId,
        input.currentGeneration,
      );
      this.sql.exec(
        `DELETE FROM connection_ticket
         WHERE peer = 'browser' AND client_id = ?`,
        input.clientId,
      );
      this.sql.exec(
        `INSERT INTO connection_ticket
          (ticket_digest, connection_set_id, connection_id, peer, channel,
           client_id, subject, role, stream_id, delivery_generation, expires_at,
           relay_capabilities_json)
         VALUES (?, ?, ?, 'browser', 'data', ?, ?, ?, ?, ?, ?, ?)`,
        input.ticketDigest,
        input.connectionSetId,
        input.connectionId,
        input.clientId,
        input.subject,
        input.role,
        input.streamId,
        input.nextGeneration,
        input.expiresAt,
        input.relayCapabilitiesJson,
      );
      replaced = true;
    });
    return replaced;
  }

  #reserveClient(
    clientId: string,
    principalIdHash: string,
    role: "writer" | "observer",
    expiresAt: number,
    now: number,
  ): ClientReservationResult {
    const existing = this.store.clientById(clientId);
    if (existing !== undefined) {
      if (existing.principal_id_hash !== principalIdHash || existing.role !== role) {
        return { ok: false, reason: "client-owner-mismatch" };
      }
      if (existing.registered_at === null) {
        this.sql.exec(
          `UPDATE client_delivery SET reservation_expires_at = ?, updated_at = ?
           WHERE client_id = ? AND registered_at IS NULL`,
          expiresAt,
          Date.now(),
          clientId,
        );
      }
      return { client: this.#requiredClient(clientId), ok: true };
    }

    let clientCount = this.sql.exec("SELECT COUNT(*) AS value FROM client_delivery").one() as {
      value: number;
    };
    if (clientCount.value >= this.maxBrowserClients && this.#evictInactiveClient(now)) {
      clientCount = this.sql.exec("SELECT COUNT(*) AS value FROM client_delivery").one() as {
        value: number;
      };
    }
    if (clientCount.value >= this.maxBrowserClients) {
      return { ok: false, reason: "too-many-clients" };
    }
    const session = this.store.session();
    if (session === undefined) throw new Error("session disappeared while reserving client");
    const streamId = session.next_stream_id;
    if (streamId < 1 || streamId > 0xffff_ffff) {
      throw new Error("stream id space exhausted");
    }
    this.sql.exec(
      `INSERT INTO client_delivery
        (client_id, principal_id_hash, role, stream_id, delivery_generation,
         registered_at, reservation_expires_at, updated_at)
       VALUES (?, ?, ?, ?, '1', NULL, ?, ?)`,
      clientId,
      principalIdHash,
      role,
      streamId,
      expiresAt,
      now,
    );
    this.sql.exec(
      "UPDATE session_state SET next_stream_id = ?, updated_at = ? WHERE singleton = 1",
      streamId + 1,
      now,
    );
    return { client: this.#requiredClient(clientId), ok: true };
  }

  #requiredClient(clientId: string): ClientRow {
    const client = this.store.clientById(clientId);
    if (client === undefined) throw new Error("reserved client disappeared");
    return client;
  }

  #reservedDeliveryGeneration(client: ClientRow | undefined): string {
    if (client === undefined || client.registered_at === null) {
      return client?.delivery_generation ?? "0";
    }
    const currentGeneration = BigInt(client.delivery_generation);
    if (currentGeneration >= MAX_U64) throw new Error("delivery generation space exhausted");
    return (currentGeneration + 1n).toString();
  }

  #evictInactiveClient(now: number): boolean {
    const candidates = this.sql
      .exec("SELECT * FROM client_delivery ORDER BY updated_at ASC")
      .toArray() as unknown as ClientRow[];
    for (const candidate of candidates) {
      const hasSocket = this.state
        .getWebSockets(`client:${candidate.client_id}`)
        .some((socket) => socket.readyState < WebSocket.CLOSING);
      if (hasSocket) continue;
      const pendingTicket = this.sql
        .exec(
          "SELECT 1 AS value FROM connection_ticket WHERE client_id = ? LIMIT 1",
          candidate.client_id,
        )
        .toArray()[0];
      if (pendingTicket !== undefined) continue;
      const lease = this.store.writerLease();
      if (lease?.client_id === candidate.client_id && lease.expires_at > now) continue;

      this.sql.exec(
        `DELETE FROM client_delivery
         WHERE client_id = ?
           AND NOT EXISTS (SELECT 1 FROM connection_ticket WHERE client_id = ?)
           AND NOT EXISTS (
             SELECT 1 FROM writer_lease WHERE client_id = ? AND expires_at > ?
           )`,
        candidate.client_id,
        candidate.client_id,
        candidate.client_id,
        now,
      );
      if (this.store.clientById(candidate.client_id) === undefined) {
        return true;
      }
    }
    return false;
  }

  #cleanupExpired(now: number): void {
    this.sql.exec("DELETE FROM connection_ticket WHERE expires_at <= ?", now);
    this.sql.exec(
      `DELETE FROM client_delivery
       WHERE registered_at IS NULL AND reservation_expires_at <= ?`,
      now,
    );
  }

  #revokePending(peer: "host" | "browser", clientId: string | null, subject: string): void {
    if (peer === "browser") {
      this.sql.exec(
        "DELETE FROM connection_ticket WHERE peer = 'browser' AND client_id = ?",
        clientId,
      );
      return;
    }
    this.sql.exec("DELETE FROM connection_ticket WHERE peer = 'host' AND subject = ?", subject);
  }
}
