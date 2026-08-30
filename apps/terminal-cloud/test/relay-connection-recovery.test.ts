import { env, exports as workerExports } from "cloudflare:workers";
import { runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import {
  RelayConnectionStore,
  type RelayConnectionRecoveryLifecycle,
} from "../src/worker/relay-connection-store";
import { RelayStore } from "../src/worker/relay-store";

const origin = "https://terminal.example.test";
const engineId = "ghostty:test+snapshot-v1+wterm:test";
let sessionCounter = 0;

async function createSession(): Promise<string> {
  sessionCounter += 1;
  const sessionId = `session_connection_recovery_${sessionCounter.toString().padStart(16, "0")}`;
  const response = await workerExports.default.fetch(
    new Request(`${origin}/api/v1/sessions`, {
      method: "POST",
      headers: {
        authorization: "Bearer test-bootstrap-token-with-at-least-32-bytes",
        "content-type": "application/json",
      },
      body: JSON.stringify({ sessionId, engineId, sessionEpoch: "7" }),
    }),
  );
  expect(response.status).toBe(201);
  await response.body?.cancel();
  return sessionId;
}

function sessionStub(sessionId: string) {
  return env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${sessionId}`));
}

class RecoveryFenceProbe implements RelayConnectionRecoveryLifecycle {
  readonly calls: string[] = [];
  fencedIds: readonly string[] = ["recovery_fenced_0000000001"];
  shouldThrow = false;

  constructor(private readonly sql: SqlStorage) {}

  fenceClientGeneration(clientId: string, generation: string, now: number): readonly string[] {
    return this.record(`client:${clientId}:${generation}:${now}`);
  }

  fenceHost(hostFence: string, now: number): readonly string[] {
    return this.record(`host:${hostFence}:${now}`);
  }

  fenceRemovedClient(clientId: string, now: number): readonly string[] {
    return this.record(`removed:${clientId}:${now}`);
  }

  private record(call: string): readonly string[] {
    this.calls.push(call);
    this.sql.exec("UPDATE session_state SET snapshot_retention_backlog = 99 WHERE singleton = 1");
    if (this.shouldThrow) throw new Error("injected recovery fence capacity failure");
    return this.fencedIds;
  }
}

function connectionStore(
  durable: DurableObjectState,
  recoveries: RelayConnectionRecoveryLifecycle,
  onRecoveriesFenced: () => void,
  maxBrowserClients = 16,
): RelayConnectionStore {
  return new RelayConnectionStore(
    durable,
    durable.storage.sql,
    new RelayStore(durable.storage.sql),
    recoveries,
    onRecoveriesFenced,
    maxBrowserClients,
    maxBrowserClients + 1,
  );
}

function seedClient(
  durable: DurableObjectState,
  clientId: string,
  generation = "1",
  streamId = 1,
): void {
  durable.storage.sql.exec(
    `INSERT INTO client_delivery
      (client_id, principal_id_hash, role, stream_id, delivery_generation,
       updated_at, registered_at, reservation_expires_at)
     VALUES (?, ?, 'observer', ?, ?, 1, 1, NULL)`,
    clientId,
    `principal_${clientId}`,
    streamId,
    generation,
  );
}

function seedTicket(
  durable: DurableObjectState,
  input: {
    channel: "control" | "data";
    clientId: string | null;
    connectionId: string;
    connectionSetId: string;
    deliveryGeneration: string;
    hostFence?: string;
    peer: "browser" | "host";
    role: "host" | "observer";
    streamId: number;
    ticketDigest: string;
  },
): void {
  durable.storage.sql.exec(
    `INSERT INTO connection_ticket
      (ticket_digest, connection_set_id, connection_id, peer, channel, client_id,
       subject, role, stream_id, delivery_generation, host_fence, expires_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    input.ticketDigest,
    input.connectionSetId,
    input.connectionId,
    input.peer,
    input.channel,
    input.clientId,
    `subject_${input.connectionId}`,
    input.role,
    input.streamId,
    input.deliveryGeneration,
    input.hostFence ?? null,
    Date.now() + 60_000,
  );
}

function sessionFacts(durable: DurableObjectState): {
  host_fence: string;
  next_stream_id: number;
  snapshot_retention_backlog: number;
} {
  return durable.storage.sql
    .exec(
      `SELECT host_fence, next_stream_id, snapshot_retention_backlog
       FROM session_state WHERE singleton = 1`,
    )
    .one() as {
    host_fence: string;
    next_stream_id: number;
    snapshot_retention_backlog: number;
  };
}

describe("RelayConnectionStore recovery fences", () => {
  it("commits host fences with recovery cleanup and rolls both back on capacity failure", async () => {
    const sessionId = await createSession();
    await runInDurableObject(sessionStub(sessionId), (_instance, durable) => {
      durable.storage.sql.exec(
        "UPDATE session_state SET host_fence = '3', snapshot_retention_backlog = 1 WHERE singleton = 1",
      );
      seedTicket(durable, {
        channel: "control",
        clientId: null,
        connectionId: "connection_host_recovery_0001",
        connectionSetId: "connection_set_host_recovery_0001",
        deliveryGeneration: "0",
        peer: "host",
        role: "host",
        streamId: 0,
        ticketDigest: "ticket_host_recovery_00000001",
      });
      const recoveries = new RecoveryFenceProbe(durable.storage.sql);
      recoveries.fencedIds = [];
      let maintenanceSignals = 0;
      const connections = connectionStore(durable, recoveries, () => {
        maintenanceSignals += 1;
      });

      expect(connections.advanceHostFence("connection_set_host_recovery_0001")).toBe("4");
      expect(sessionFacts(durable)).toMatchObject({
        host_fence: "4",
        snapshot_retention_backlog: 99,
      });
      expect(recoveries.calls.at(-1)).toMatch(/^host:4:/);
      expect(maintenanceSignals).toBe(1);

      durable.storage.sql.exec(
        "UPDATE session_state SET snapshot_retention_backlog = 1 WHERE singleton = 1",
      );
      recoveries.shouldThrow = true;
      expect(() =>
        connections.invalidateHostConnection("connection_set_host_recovery_0001", "4"),
      ).toThrow("injected recovery fence capacity failure");
      expect(sessionFacts(durable)).toMatchObject({
        host_fence: "4",
        snapshot_retention_backlog: 1,
      });
      expect(
        durable.storage.sql
          .exec(
            "SELECT host_fence FROM connection_ticket WHERE ticket_digest = 'ticket_host_recovery_00000001'",
          )
          .one(),
      ).toEqual({ host_fence: "4" });
      expect(maintenanceSignals).toBe(1);
    });
  });

  it("fences the activated browser generation in the same claim transaction", async () => {
    const sessionId = await createSession();
    await runInDurableObject(sessionStub(sessionId), (_instance, durable) => {
      const clientId = "client_claim_recovery_00000001";
      seedClient(durable, clientId);
      seedTicket(durable, {
        channel: "control",
        clientId,
        connectionId: "connection_claim_recovery_0001",
        connectionSetId: "connection_set_claim_recovery_0001",
        deliveryGeneration: "2",
        hostFence: "7",
        peer: "browser",
        role: "observer",
        streamId: 1,
        ticketDigest: "ticket_claim_recovery_00000001",
      });
      seedTicket(durable, {
        channel: "data",
        clientId,
        connectionId: "connection_claim_recovery_0001",
        connectionSetId: "connection_set_claim_recovery_0001",
        deliveryGeneration: "2",
        hostFence: "7",
        peer: "browser",
        role: "observer",
        streamId: 1,
        ticketDigest: "ticket_claim_recovery_data_0001",
      });
      const relay = new RelayStore(durable.storage.sql);
      const ticket = relay.ticket("ticket_claim_recovery_00000001");
      if (ticket === undefined) throw new Error("claim ticket missing");
      const recoveries = new RecoveryFenceProbe(durable.storage.sql);
      recoveries.shouldThrow = true;
      let maintenanceSignals = 0;
      const retentionBacklogBefore = sessionFacts(durable).snapshot_retention_backlog;
      const connections = connectionStore(durable, recoveries, () => {
        maintenanceSignals += 1;
      });

      durable.storage.sql.exec(
        "DELETE FROM connection_ticket WHERE connection_set_id = ? AND channel = 'data'",
        ticket.connection_set_id,
      );
      expect(connections.claimBrowserControl(ticket, "7")).toBeUndefined();
      seedTicket(durable, {
        channel: "data",
        clientId,
        connectionId: "connection_claim_recovery_0001",
        connectionSetId: "connection_set_claim_recovery_0001",
        deliveryGeneration: "2",
        hostFence: "7",
        peer: "browser",
        role: "observer",
        streamId: 1,
        ticketDigest: "ticket_claim_recovery_data_0002",
      });
      expect(connections.claimBrowserControl(ticket, "8")).toBeUndefined();
      expect(relay.clientById(clientId)).toMatchObject({
        delivery_generation: "1",
      });
      expect(() => connections.claimBrowserControl(ticket, "7")).toThrow(
        "injected recovery fence capacity failure",
      );
      expect(relay.clientById(clientId)).toMatchObject({
        delivery_generation: "1",
      });
      expect(sessionFacts(durable).snapshot_retention_backlog).toBe(retentionBacklogBefore);
      expect(maintenanceSignals).toBe(0);

      recoveries.shouldThrow = false;
      recoveries.fencedIds = [];
      expect(connections.claimBrowserControl(ticket, "7")).toMatchObject({
        delivery_generation: "2",
      });
      expect(relay.clientById(clientId)).toMatchObject({
        delivery_generation: "2",
      });
      expect(recoveries.calls.at(-1)).toMatch(new RegExp(`^client:${clientId}:2:`));
      expect(maintenanceSignals).toBe(1);
    });
  });

  it("fences an inactive client before deleting it and rolls back the whole reservation", async () => {
    const sessionId = await createSession();
    await runInDurableObject(sessionStub(sessionId), (_instance, durable) => {
      const oldClientId = "client_evicted_recovery_000001";
      const newClientId = "client_reserved_recovery_00001";
      seedClient(durable, oldClientId);
      const recoveries = new RecoveryFenceProbe(durable.storage.sql);
      recoveries.shouldThrow = true;
      let maintenanceSignals = 0;
      const connections = connectionStore(
        durable,
        recoveries,
        () => {
          maintenanceSignals += 1;
        },
        1,
      );
      const nextStreamBefore = sessionFacts(durable).next_stream_id;
      const retentionBacklogBefore = sessionFacts(durable).snapshot_retention_backlog;
      const reservation = {
        clientId: newClientId,
        connectionId: "connection_reserved_recovery_0001",
        connectionSetId: "connection_set_reserved_recovery_0001",
        controlTicketDigest: "ticket_reserved_control_00000001",
        dataTicketDigest: "ticket_reserved_data_0000000001",
        expiresAt: Date.now() + 60_000,
        now: Date.now(),
        peer: "browser" as const,
        principalIdHash: `principal_${newClientId}`,
        hostFenceWitness: "0",
        role: "observer" as const,
        subject: "subject_reserved_recovery_000001",
      };

      expect(() => connections.reserveConnectionSet(reservation)).toThrow(
        "injected recovery fence capacity failure",
      );
      const relay = new RelayStore(durable.storage.sql);
      expect(relay.clientById(oldClientId)).toBeDefined();
      expect(relay.clientById(newClientId)).toBeUndefined();
      expect(sessionFacts(durable)).toMatchObject({
        next_stream_id: nextStreamBefore,
        snapshot_retention_backlog: retentionBacklogBefore,
      });
      expect(maintenanceSignals).toBe(0);

      recoveries.shouldThrow = false;
      recoveries.fencedIds = [];
      expect(connections.reserveConnectionSet(reservation)).toMatchObject({ ok: true });
      expect(relay.clientById(oldClientId)).toBeUndefined();
      expect(relay.clientById(newClientId)).toBeDefined();
      expect(recoveries.calls.at(-1)).toMatch(new RegExp(`^removed:${oldClientId}:`));
      expect(maintenanceSignals).toBe(1);
    });
  });
});
