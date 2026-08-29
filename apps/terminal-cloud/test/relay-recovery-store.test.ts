import type {
  AuthorityCursor,
  RecoveryAdopted,
  RecoveryStart,
  RecoveryStartFence,
  RecoveryV3HostPrepare,
  RecoveryV3HostSourceClosed,
} from "@zhongduan/protocol";
import {
  DataFrameFlag,
  DataFrameKind,
  encodeDataFrame,
  encodeDeliveryEnvelopeV3,
} from "@zhongduan/protocol";
import { env, exports as workerExports } from "cloudflare:workers";
import { evictDurableObject, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import {
  RelayRecoveryStore,
  type RelayRecoveryStoreLimits,
} from "../src/worker/relay-recovery-store";
import { RelayStore } from "../src/worker/relay-store";

const origin = "https://terminal.example.test";
const engineId = "ghostty:test+snapshot-v1+wterm:test";
const limits: RelayRecoveryStoreLimits = {
  maxAttempts: 16,
  maxDeliveryEncodedBytes: 2 * 1024 * 1024,
  maxDeliveryRecords: 1024,
  maxOutboxEntries: 64,
};
let sessionCounter = 0;

interface SessionFixture {
  clientId: string;
  connectionId: string;
  recoveryId: string;
  sessionId: string;
}

async function createSession(): Promise<SessionFixture> {
  sessionCounter += 1;
  const suffix = sessionCounter.toString().padStart(16, "0");
  const sessionId = `session_recovery_${suffix}`;
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
  return {
    clientId: `client_recovery_${suffix}`,
    connectionId: `connection_recovery_${suffix}`,
    recoveryId: `recovery_attempt_${suffix}`,
    sessionId,
  };
}

function sessionStub(sessionId: string) {
  return env.TERMINAL_SESSIONS.get(env.TERMINAL_SESSIONS.idFromName(`v1:${sessionId}`));
}

function seedClient(
  durable: DurableObjectState,
  fixture: SessionFixture,
  strategy: "v2" | "v3",
  generation = "1",
  streamId = 1,
): void {
  durable.storage.sql.exec(
    `INSERT INTO client_delivery
      (client_id, principal_id_hash, role, stream_id, delivery_generation,
       updated_at, registered_at, reservation_expires_at, recovery_strategy)
     VALUES (?, ?, 'observer', ?, ?, 1, 1, NULL, ?)`,
    fixture.clientId,
    `principal_${fixture.clientId}`,
    streamId,
    generation,
    strategy,
  );
}

function installAuthority(
  durable: DurableObjectState,
  head: AuthorityCursor,
  hostFence = "3",
): void {
  durable.storage.sql.exec(
    `UPDATE session_state
     SET host_fence = ?, head_event_seq = ?, next_pty_offset = ?, updated_at = 1
     WHERE singleton = 1`,
    hostFence,
    head.eventSeq,
    head.nextPtyOffset,
  );
}

function recoveryStore(
  durable: DurableObjectState,
  storeLimits: RelayRecoveryStoreLimits = limits,
): RelayRecoveryStore {
  return new RelayRecoveryStore(
    durable.storage.sql,
    new RelayStore(durable.storage.sql),
    storeLimits,
  );
}

function transaction<T>(durable: DurableObjectState, operation: () => T): T {
  return durable.storage.transactionSync(operation);
}

function sendValidatedLaneDelivery(
  store: RelayRecoveryStore,
  recoveryId: string,
  encoded: ArrayBuffer | Uint8Array,
  now: number,
) {
  const queued = store.enqueueValidatedLaneDelivery(recoveryId, encoded, now);
  if (!queued.ok) return queued;
  const begun = store.beginLaneDeliverySend(queued.record, now);
  if (!begun.ok) return begun;
  return store.confirmLaneDeliverySend(queued.record, now);
}

const base: AuthorityCursor = { sessionEpoch: "7", eventSeq: "2", nextPtyOffset: "4" };
const committed: AuthorityCursor = {
  sessionEpoch: "7",
  eventSeq: "5",
  nextPtyOffset: "12",
};

function prepareFor(fixture: SessionFixture, generation = "1", streamId = 1) {
  return {
    type: "recovery-prepare",
    recoveryId: fixture.recoveryId,
    connectionId: fixture.connectionId,
    streamId,
    deliveryGeneration: generation,
    engineId,
    base,
    source: { kind: "warm" },
  } satisfies RecoveryV3HostPrepare;
}

function fenceFor(fixture: SessionFixture, generation = "1", streamId = 1) {
  return {
    type: "recovery-start-fence",
    recoveryId: fixture.recoveryId,
    connectionId: fixture.connectionId,
    deliveryGeneration: generation,
    streamId,
    engineId,
    base,
    source: { kind: "warm" },
    committedThrough: committed,
    liveFloor: { sessionEpoch: "7", nextEventSeq: "6", nextPtyOffset: "12" },
  } satisfies RecoveryStartFence;
}

function startFor(fixture: SessionFixture, generation = "1", streamId = 1) {
  return {
    type: "recovery-start",
    recoveryId: fixture.recoveryId,
    deliveryGeneration: generation,
    streamId,
    engineId,
    authorityDataVersion: 2,
    base,
    source: { kind: "warm" },
    committedThrough: committed,
    liveFloor: { sessionEpoch: "7", nextEventSeq: "6", nextPtyOffset: "12" },
  } satisfies RecoveryStart;
}

function beginInput(fixture: SessionFixture, hardDeadlineAt = 10_000) {
  return {
    clientId: fixture.clientId,
    hardDeadlineAt,
    hostFence: "3",
    noProgressTimeoutMs: 1_000,
    now: 100,
    prepare: prepareFor(fixture),
  };
}

function installInput(fixture: SessionFixture) {
  return {
    cumulativeGrantedEncodedBytes: "1000",
    fence: fenceFor(fixture),
    now: 200,
    start: startFor(fixture),
  };
}

function deliveryEnvelope(
  lane: "live" | "recovery",
  ordinal: bigint,
  cumulativeEncodedBytes: bigint,
  frame: {
    eventSeq: bigint;
    kind: typeof DataFrameKind.PtyOutput | typeof DataFrameKind.ReplayCommit;
    payload: Uint8Array;
    ptyOffset: bigint;
  },
): Uint8Array {
  return encodeDeliveryEnvelopeV3({
    lane,
    deliveryGeneration: 1n,
    deliveryOrdinal: ordinal,
    cumulativeEncodedBytes,
    streamId: 1,
    payload: encodeDataFrame({
      kind: frame.kind,
      flags: DataFrameFlag.None,
      sessionEpoch: 7n,
      deliveryGeneration: 0n,
      eventSeq: frame.eventSeq,
      ptyOffset: frame.ptyOffset,
      streamId: 0,
      payload: frame.payload,
    }),
  });
}

describe("RelayRecoveryStore", () => {
  it("migrates an intact v6 relay through the bounded strict v9 ledger and keeps v2 defaulted", async () => {
    const fixture = await createSession();
    const stub = sessionStub(fixture.sessionId);
    await runInDurableObject(stub, (_instance, durable) => {
      seedClient(durable, fixture, "v2");
      durable.storage.sql.exec("DROP TABLE recovery_control_outbox");
      durable.storage.sql.exec("DROP TABLE recovery_delivery_lane");
      durable.storage.sql.exec("DROP TABLE recovery_attempt");
      durable.storage.kv.put("schema-version", 6);
    });
    await evictDurableObject(stub);

    await runInDurableObject(stub, (_instance, durable) => {
      const sql = durable.storage.sql;
      expect(durable.storage.kv.get<number>("schema-version")).toBe(9);
      expect(
        sql
          .exec(
            `SELECT name, strict FROM pragma_table_list
             WHERE name LIKE 'recovery_%' ORDER BY name`,
          )
          .toArray(),
      ).toEqual([
        { name: "recovery_attempt", strict: 1 },
        { name: "recovery_control_outbox", strict: 1 },
        { name: "recovery_delivery_lane", strict: 1 },
        { name: "recovery_delivery_record", strict: 1 },
      ]);
      expect(
        sql
          .exec(
            "SELECT recovery_strategy FROM client_delivery WHERE client_id = ?",
            fixture.clientId,
          )
          .one(),
      ).toEqual({ recovery_strategy: "v2" });
      installAuthority(durable, committed);
      const result = transaction(durable, () =>
        recoveryStore(durable).beginPreparing(beginInput(fixture)),
      );
      expect(result).toEqual({ ok: false, reason: "client-v2" });
      expect(sql.exec("SELECT COUNT(*) AS value FROM recovery_attempt").one()).toEqual({
        value: 0,
      });
    });
  });

  it("persists prepare before outbox, rejects divergence, and installs one exact fence", async () => {
    const fixture = await createSession();
    await runInDurableObject(sessionStub(fixture.sessionId), (_instance, durable) => {
      seedClient(durable, fixture, "v3");
      installAuthority(durable, committed);
      const store = recoveryStore(durable);

      expect(transaction(durable, () => store.beginPreparing(beginInput(fixture)))).toEqual({
        changed: true,
        ok: true,
      });
      expect(store.attempt(fixture.recoveryId)).toMatchObject({
        host_fence: "3",
        state: "preparing",
      });
      expect(store.outbox()).toMatchObject([
        { destination: "host", kind: "recovery-prepare", recovery_id: fixture.recoveryId },
      ]);
      expect(transaction(durable, () => store.beginPreparing(beginInput(fixture)))).toEqual({
        changed: false,
        ok: true,
      });
      expect(
        transaction(durable, () =>
          store.beginPreparing({
            ...beginInput(fixture),
            prepare: {
              ...prepareFor(fixture),
              source: { kind: "snapshot", snapshotId: "snapshot_recovery_0001" },
            },
          }),
        ),
      ).toEqual({ ok: false, reason: "immutable-conflict" });

      expect(transaction(durable, () => store.installFence(installInput(fixture)))).toEqual({
        changed: true,
        ok: true,
      });
      expect(store.attempt(fixture.recoveryId)).toMatchObject({
        granted_cumulative_encoded_bytes: "1000",
        state: "installed",
      });
      expect(store.recoveryGrantReservation(fixture.recoveryId)).toBe("1000");
      expect(store.lanes(fixture.recoveryId)).toMatchObject([
        {
          lane: "live",
          received_cumulative_encoded_bytes: "0",
          received_delivery_ordinal: "0",
          sent_authority_cursor_json: JSON.stringify(committed),
        },
        {
          lane: "recovery",
          received_cumulative_encoded_bytes: "0",
          received_delivery_ordinal: "0",
          sent_authority_cursor_json: JSON.stringify(base),
        },
      ]);
      expect(store.outbox().map((entry) => entry.kind)).toEqual([
        "recovery-start",
        "recovery-start-ready",
      ]);

      installAuthority(durable, { sessionEpoch: "7", eventSeq: "6", nextPtyOffset: "15" });
      expect(transaction(durable, () => store.installFence(installInput(fixture)))).toEqual({
        changed: false,
        ok: true,
      });
      expect(
        transaction(durable, () =>
          store.installFence({
            ...installInput(fixture),
            start: {
              ...startFor(fixture),
              base: { sessionEpoch: "7", eventSeq: "1", nextPtyOffset: "1" },
            },
          }),
        ),
      ).toEqual({ ok: false, reason: "immutable-conflict" });

      let blobRejected = false;
      try {
        durable.storage.sql.exec(
          `INSERT INTO recovery_control_outbox
            (recovery_id, kind, destination, payload_json, created_at, updated_at)
           VALUES (?, 'recovery-source-grant', 'host', zeroblob(8), 1, 1)`,
          fixture.recoveryId,
        );
      } catch {
        blobRejected = true;
      }
      expect(blobRejected).toBe(true);
    });
  });

  it("locates attempts and outbox entries only by exact durable identity", async () => {
    const fixture = await createSession();
    await runInDurableObject(sessionStub(fixture.sessionId), (_instance, durable) => {
      seedClient(durable, fixture, "v3");
      installAuthority(durable, committed);
      const store = recoveryStore(durable);
      transaction(durable, () => store.beginPreparing(beginInput(fixture)));

      const attempt = store.attempt(fixture.recoveryId);
      expect(store.attemptByDeliveryIdentity(1, "1")).toEqual(attempt);
      expect(
        store.attemptByConnectionIdentity(fixture.clientId, fixture.connectionId, 1, "1"),
      ).toEqual(attempt);

      const duplicateColumns = `
        (recovery_id, client_id, connection_id, host_fence, stream_id, delivery_generation,
         engine_id, state, prepare_json, start_json, base_cursor_json, committed_through_json,
         live_floor_json, granted_cumulative_encoded_bytes, recovery_done_through_json,
         recovery_done_ordinal, recovery_done_cumulative_encoded_bytes, replica_applied_json,
         adopted_json, source_closed_json, hard_deadline_at, no_progress_timeout_ms,
         no_progress_deadline_at, reset_reason, created_at, updated_at)`;
      const duplicateProjection = `
        SELECT ?, ?, ?, host_fence, stream_id, delivery_generation,
               engine_id, state, prepare_json, start_json, base_cursor_json,
               committed_through_json, live_floor_json, granted_cumulative_encoded_bytes,
               recovery_done_through_json, recovery_done_ordinal,
               recovery_done_cumulative_encoded_bytes, replica_applied_json, adopted_json,
               source_closed_json, hard_deadline_at, no_progress_timeout_ms,
               no_progress_deadline_at, reset_reason, created_at, updated_at
        FROM recovery_attempt WHERE recovery_id = ?`;
      const duplicateRecoveryId = `${fixture.recoveryId}_delivery_duplicate`;
      durable.storage.sql.exec(
        `INSERT INTO recovery_attempt ${duplicateColumns} ${duplicateProjection}`,
        duplicateRecoveryId,
        `${fixture.clientId}_delivery_duplicate`,
        `${fixture.connectionId}_delivery_duplicate`,
        fixture.recoveryId,
      );
      expect(store.attemptByDeliveryIdentity(1, "1")).toBeUndefined();
      expect(
        store.attemptByConnectionIdentity(fixture.clientId, fixture.connectionId, 1, "1"),
      ).toEqual(attempt);
      durable.storage.sql.exec(
        "DELETE FROM recovery_attempt WHERE recovery_id = ?",
        duplicateRecoveryId,
      );

      // The schema's (client_id, delivery_generation) uniqueness prevents an
      // exact reconnect identity duplicate before the bounded lookup runs.
      expect(() =>
        durable.storage.sql.exec(
          `INSERT INTO recovery_attempt ${duplicateColumns} ${duplicateProjection}`,
          `${fixture.recoveryId}_connection_duplicate`,
          fixture.clientId,
          fixture.connectionId,
          fixture.recoveryId,
        ),
      ).toThrow();
      expect(store.attemptByDeliveryIdentity(2, "1")).toBeUndefined();
      expect(store.attemptByDeliveryIdentity(1, "2")).toBeUndefined();
      expect(
        store.attemptByConnectionIdentity(
          `${fixture.clientId}_other`,
          fixture.connectionId,
          1,
          "1",
        ),
      ).toBeUndefined();
      expect(
        store.attemptByConnectionIdentity(
          fixture.clientId,
          `${fixture.connectionId}_other`,
          1,
          "1",
        ),
      ).toBeUndefined();

      const prepare = store.outboxEntry(fixture.recoveryId, "recovery-prepare");
      expect(prepare).toMatchObject({
        destination: "host",
        kind: "recovery-prepare",
        recovery_id: fixture.recoveryId,
      });
      expect(store.outboxEntry(fixture.recoveryId, "recovery-start")).toBeUndefined();
      expect(store.outboxEntry(`${fixture.recoveryId}_other`, "recovery-prepare")).toBeUndefined();

      expect(store.attempt(fixture.recoveryId)).toEqual(attempt);
      expect(store.outbox()).toEqual([prepare]);
    });
  });

  it("keeps lane cursors independent and fails closed when a sent ordinal cannot be authenticated", async () => {
    const fixture = await createSession();
    await runInDurableObject(sessionStub(fixture.sessionId), (_instance, durable) => {
      seedClient(durable, fixture, "v3");
      installAuthority(durable, committed);
      const store = recoveryStore(durable);
      transaction(durable, () => store.beginPreparing(beginInput(fixture)));
      transaction(durable, () => store.installFence(installInput(fixture)));
      installAuthority(durable, { sessionEpoch: "7", eventSeq: "6", nextPtyOffset: "14" });

      const liveAtH = deliveryEnvelope("live", 1n, 90n, {
        eventSeq: 5n,
        kind: DataFrameKind.PtyOutput,
        payload: new Uint8Array([1, 2]),
        ptyOffset: 12n,
      });
      const liveSkipped = deliveryEnvelope("live", 1n, 90n, {
        eventSeq: 7n,
        kind: DataFrameKind.PtyOutput,
        payload: new Uint8Array([1, 2]),
        ptyOffset: 12n,
      });
      const liveNext = deliveryEnvelope("live", 1n, 90n, {
        eventSeq: 6n,
        kind: DataFrameKind.PtyOutput,
        payload: new Uint8Array([1, 2]),
        ptyOffset: 12n,
      });
      expect(
        transaction(durable, () =>
          sendValidatedLaneDelivery(store, fixture.recoveryId, liveAtH, 210),
        ),
      ).toEqual({ ok: false, reason: "invalid-progress" });
      expect(
        transaction(durable, () =>
          sendValidatedLaneDelivery(store, fixture.recoveryId, liveSkipped, 211),
        ),
      ).toEqual({ ok: false, reason: "invalid-progress" });
      expect(
        transaction(durable, () =>
          sendValidatedLaneDelivery(store, fixture.recoveryId, liveNext, 212),
        ),
      ).toEqual({ changed: true, ok: true });
      // No durable bytes/hash exist to prove an equal ordinal is an exact retry.
      expect(
        transaction(durable, () =>
          sendValidatedLaneDelivery(store, fixture.recoveryId, liveNext, 213),
        ),
      ).toEqual({ ok: false, reason: "invalid-progress" });

      for (const [index, encoded] of [
        deliveryEnvelope("recovery", 1n, 90n, {
          eventSeq: 3n,
          kind: DataFrameKind.PtyOutput,
          payload: new Uint8Array([1, 2]),
          ptyOffset: 4n,
        }),
        deliveryEnvelope("recovery", 2n, 180n, {
          eventSeq: 4n,
          kind: DataFrameKind.PtyOutput,
          payload: new Uint8Array([3, 4]),
          ptyOffset: 6n,
        }),
        deliveryEnvelope("recovery", 3n, 272n, {
          eventSeq: 5n,
          kind: DataFrameKind.PtyOutput,
          payload: new Uint8Array([5, 6, 7, 8]),
          ptyOffset: 8n,
        }),
      ].entries()) {
        expect(
          transaction(durable, () =>
            sendValidatedLaneDelivery(store, fixture.recoveryId, encoded, 220 + index),
          ),
        ).toEqual({ changed: true, ok: true });
        if (index < 2) {
          expect(
            transaction(durable, () =>
              store.markLaneReceived(
                {
                  receipt: {
                    type: "delivery-received",
                    deliveryGeneration: "1",
                    lane: "recovery",
                    contiguousDeliveryOrdinal: (index + 1).toString(),
                    cumulativeEncodedBytes: ["90", "180"][index]!,
                  },
                  recoveryId: fixture.recoveryId,
                },
                220 + index,
              ),
            ),
          ).toEqual({ changed: true, ok: true });
        }
      }
      expect(
        transaction(durable, () =>
          sendValidatedLaneDelivery(
            store,
            fixture.recoveryId,
            deliveryEnvelope("recovery", 4n, 362n, {
              eventSeq: 6n,
              kind: DataFrameKind.PtyOutput,
              payload: new Uint8Array([9, 10]),
              ptyOffset: 12n,
            }),
            224,
          ),
        ),
      ).toEqual({ ok: false, reason: "invalid-progress" });
      expect(store.lanes(fixture.recoveryId)).toMatchObject([
        {
          lane: "live",
          sent_authority_cursor_json: JSON.stringify({
            sessionEpoch: "7",
            eventSeq: "6",
            nextPtyOffset: "14",
          }),
          sent_delivery_ordinal: "1",
        },
        {
          lane: "recovery",
          sent_authority_cursor_json: JSON.stringify(committed),
          sent_delivery_ordinal: "3",
        },
      ]);

      expect(
        transaction(durable, () =>
          store.markLaneReceived(
            {
              receipt: {
                type: "delivery-received",
                deliveryGeneration: "1",
                lane: "live",
                contiguousDeliveryOrdinal: "1",
                cumulativeEncodedBytes: "90",
              },
              recoveryId: fixture.recoveryId,
            },
            225,
          ),
        ),
      ).toEqual({ changed: true, ok: true });
      expect(
        transaction(durable, () =>
          store.markLaneReceived(
            {
              receipt: {
                type: "delivery-received",
                deliveryGeneration: "1",
                lane: "recovery",
                contiguousDeliveryOrdinal: "3",
                cumulativeEncodedBytes: "272",
              },
              recoveryId: fixture.recoveryId,
            },
            226,
          ),
        ),
      ).toEqual({ changed: true, ok: true });
      const liveCursor = { sessionEpoch: "7", eventSeq: "6", nextPtyOffset: "14" };
      expect(
        transaction(durable, () =>
          store.markReplicaApplied(fixture.recoveryId, "1", liveCursor, 227),
        ),
      ).toEqual({ ok: false, reason: "invalid-progress" });

      expect(
        transaction(durable, () =>
          sendValidatedLaneDelivery(
            store,
            fixture.recoveryId,
            deliveryEnvelope("recovery", 4n, 360n, {
              eventSeq: 5n,
              kind: DataFrameKind.ReplayCommit,
              payload: new Uint8Array(),
              ptyOffset: 12n,
            }),
            228,
          ),
        ),
      ).toEqual({ changed: true, ok: true });
      expect(
        transaction(durable, () =>
          store.markLaneReceived(
            {
              receipt: {
                type: "delivery-received",
                deliveryGeneration: "1",
                lane: "recovery",
                contiguousDeliveryOrdinal: "4",
                cumulativeEncodedBytes: "360",
              },
              recoveryId: fixture.recoveryId,
            },
            229,
          ),
        ),
      ).toEqual({ changed: true, ok: true });
      expect(
        transaction(durable, () =>
          store.markReplicaApplied(fixture.recoveryId, "1", liveCursor, 230),
        ),
      ).toEqual({ changed: true, ok: true });

      installAuthority(durable, { sessionEpoch: "7", eventSeq: "7", nextPtyOffset: "15" });
      expect(
        transaction(durable, () =>
          sendValidatedLaneDelivery(
            store,
            fixture.recoveryId,
            deliveryEnvelope("live", 2n, 179n, {
              eventSeq: 7n,
              kind: DataFrameKind.PtyOutput,
              payload: new Uint8Array([3]),
              ptyOffset: 14n,
            }),
            231,
          ),
        ),
      ).toEqual({ changed: true, ok: true });
      const secondLiveCursor = { sessionEpoch: "7", eventSeq: "7", nextPtyOffset: "15" };
      expect(
        transaction(durable, () =>
          store.markReplicaApplied(fixture.recoveryId, "1", secondLiveCursor, 232),
        ),
      ).toEqual({ ok: false, reason: "invalid-progress" });
      expect(
        transaction(durable, () =>
          store.markLaneReceived(
            {
              receipt: {
                type: "delivery-received",
                deliveryGeneration: "1",
                lane: "live",
                contiguousDeliveryOrdinal: "2",
                cumulativeEncodedBytes: "179",
              },
              recoveryId: fixture.recoveryId,
            },
            233,
          ),
        ),
      ).toEqual({ changed: true, ok: true });
      expect(
        transaction(durable, () =>
          store.markReplicaApplied(fixture.recoveryId, "1", secondLiveCursor, 234),
        ),
      ).toEqual({ changed: true, ok: true });
    });
  });

  it("owns a staged multi-record window and releases only an exact sent prefix", async () => {
    const fixture = await createSession();
    await runInDurableObject(sessionStub(fixture.sessionId), (_instance, durable) => {
      seedClient(durable, fixture, "v3");
      installAuthority(durable, committed);
      const store = recoveryStore(durable);
      transaction(durable, () => store.beginPreparing(beginInput(fixture)));
      transaction(durable, () => store.installFence(installInput(fixture)));
      installAuthority(durable, { sessionEpoch: "7", eventSeq: "8", nextPtyOffset: "16" });

      const first = deliveryEnvelope("live", 1n, 90n, {
        eventSeq: 6n,
        kind: DataFrameKind.PtyOutput,
        payload: new Uint8Array([1, 2]),
        ptyOffset: 12n,
      });
      const second = deliveryEnvelope("live", 2n, 179n, {
        eventSeq: 7n,
        kind: DataFrameKind.PtyOutput,
        payload: new Uint8Array([3]),
        ptyOffset: 14n,
      });
      const third = deliveryEnvelope("live", 3n, 268n, {
        eventSeq: 8n,
        kind: DataFrameKind.PtyOutput,
        payload: new Uint8Array([4]),
        ptyOffset: 15n,
      });
      const receipt = (ordinal: string, bytes: string) => ({
        receipt: {
          type: "delivery-received" as const,
          deliveryGeneration: "1",
          lane: "live" as const,
          contiguousDeliveryOrdinal: ordinal,
          cumulativeEncodedBytes: bytes,
        },
        recoveryId: fixture.recoveryId,
      });

      const firstQueued = transaction(durable, () =>
        store.enqueueValidatedLaneDelivery(fixture.recoveryId, first, 210),
      );
      if (!firstQueued.ok) throw new Error(firstQueued.reason);
      expect(firstQueued).toMatchObject({ changed: true, ok: true });
      expect(firstQueued.record).toMatchObject({
        cumulativeEncodedBytes: "90",
        deliveryOrdinal: "1",
        encodedBytes: 90,
        lane: "live",
      });
      expect(store.deliveryRecords(fixture.recoveryId)).toMatchObject([
        { delivery_ordinal: "1", state: "queued" },
      ]);
      expect(store.lanes(fixture.recoveryId)[0]).toMatchObject({
        lane: "live",
        sent_delivery_ordinal: "0",
      });

      // The second admission chains from the last queued obligation, not from
      // the still-unadvanced sent lane high-water.
      const secondQueued = transaction(durable, () =>
        store.enqueueValidatedLaneDelivery(fixture.recoveryId, second, 211),
      );
      if (!secondQueued.ok) throw new Error(secondQueued.reason);
      expect(secondQueued.record).toMatchObject({
        cumulativeEncodedBytes: "179",
        deliveryOrdinal: "2",
        encodedBytes: 89,
      });
      const thirdQueued = transaction(durable, () =>
        store.enqueueValidatedLaneDelivery(fixture.recoveryId, third, 211),
      );
      if (!thirdQueued.ok) throw new Error(thirdQueued.reason);
      expect(store.deliveryUsage(fixture.recoveryId)).toEqual({ encodedBytes: 268, records: 3 });
      expect(store.deliveryRecoveryIdsRequiringReset()).toEqual([fixture.recoveryId]);
      expect(
        transaction(durable, () => store.beginLaneDeliverySend(secondQueued.record, 212)),
      ).toEqual({ ok: false, reason: "state-conflict" });

      expect(
        transaction(durable, () => store.beginLaneDeliverySend(firstQueued.record, 212)),
      ).toEqual({ changed: true, ok: true });
      expect(transaction(durable, () => store.markLaneReceived(receipt("1", "90"), 212))).toEqual({
        ok: false,
        reason: "progress-ahead",
      });
      expect(store.deliveryRecords(fixture.recoveryId)).toMatchObject([
        { delivery_ordinal: "1", state: "sending" },
        { delivery_ordinal: "2", state: "queued" },
        { delivery_ordinal: "3", state: "queued" },
      ]);
      expect(
        transaction(durable, () => store.confirmLaneDeliverySend(firstQueued.record, 213)),
      ).toEqual({ changed: true, ok: true });
      expect(
        transaction(durable, () => store.beginLaneDeliverySend(secondQueued.record, 214)),
      ).toEqual({ changed: true, ok: true });
      expect(
        transaction(durable, () => store.confirmLaneDeliverySend(secondQueued.record, 215)),
      ).toEqual({ changed: true, ok: true });
      expect(
        transaction(durable, () => store.beginLaneDeliverySend(thirdQueued.record, 216)),
      ).toEqual({ changed: true, ok: true });
      expect(
        transaction(durable, () => store.confirmLaneDeliverySend(thirdQueued.record, 217)),
      ).toEqual({ changed: true, ok: true });
      expect(store.deliveryRecoveryIdsRequiringReset()).toEqual([]);
      expect(store.deliveryRecordsAwaitingReceipt(fixture.recoveryId)).toHaveLength(3);

      // Unknown, mixed, and ahead scalar pairs cannot authenticate a ledger row.
      expect(transaction(durable, () => store.markLaneReceived(receipt("2", "178"), 215))).toEqual({
        ok: false,
        reason: "invalid-progress",
      });
      expect(transaction(durable, () => store.markLaneReceived(receipt("1", "179"), 216))).toEqual({
        ok: false,
        reason: "invalid-progress",
      });
      expect(transaction(durable, () => store.markLaneReceived(receipt("4", "269"), 217))).toEqual({
        ok: false,
        reason: "progress-ahead",
      });
      expect(transaction(durable, () => store.markLaneReceived(receipt("2", "179"), 218))).toEqual({
        changed: true,
        ok: true,
      });
      expect(store.deliveryRecords(fixture.recoveryId)).toMatchObject([
        { delivery_ordinal: "3", state: "sent" },
      ]);
      expect(store.deliveryUsage(fixture.recoveryId)).toEqual({ encodedBytes: 89, records: 1 });
      expect(store.lanes(fixture.recoveryId)[0]).toMatchObject({
        lane: "live",
        received_authority_cursor_json: JSON.stringify({
          sessionEpoch: "7",
          eventSeq: "7",
          nextPtyOffset: "15",
        }),
        received_delivery_ordinal: "2",
        sent_delivery_ordinal: "3",
      });
      expect(transaction(durable, () => store.markLaneReceived(receipt("3", "268"), 219))).toEqual({
        changed: true,
        ok: true,
      });

      expect(store.deliveryUsage(fixture.recoveryId)).toEqual({ encodedBytes: 0, records: 0 });
      expect(store.deliveryRecordsAwaitingReceipt(fixture.recoveryId)).toEqual([]);
      expect(transaction(durable, () => store.markLaneReceived(receipt("1", "90"), 220))).toEqual({
        ok: false,
        reason: "invalid-progress",
      });
      expect(transaction(durable, () => store.markLaneReceived(receipt("1", "91"), 221))).toEqual({
        ok: false,
        reason: "invalid-progress",
      });
      expect(transaction(durable, () => store.markLaneReceived(receipt("3", "268"), 222))).toEqual({
        changed: false,
        ok: true,
      });
      expect(store.lanes(fixture.recoveryId)[0]).toMatchObject({
        lane: "live",
        received_cumulative_encoded_bytes: "268",
        received_delivery_ordinal: "3",
        sent_cumulative_encoded_bytes: "268",
        sent_delivery_ordinal: "3",
      });
    });
  });

  it("enforces literal aggregate record and encoded-byte caps across both lanes", async () => {
    const recordFixture = await createSession();
    await runInDurableObject(sessionStub(recordFixture.sessionId), (_instance, durable) => {
      seedClient(durable, recordFixture, "v3");
      installAuthority(durable, committed);
      const store = recoveryStore(durable, {
        maxAttempts: 16,
        maxDeliveryEncodedBytes: 2 * 1024 * 1024,
        maxDeliveryRecords: 2,
        maxOutboxEntries: 64,
      });
      transaction(durable, () => store.beginPreparing(beginInput(recordFixture)));
      transaction(durable, () => store.installFence(installInput(recordFixture)));
      installAuthority(durable, { sessionEpoch: "7", eventSeq: "7", nextPtyOffset: "15" });
      const liveFirst = deliveryEnvelope("live", 1n, 90n, {
        eventSeq: 6n,
        kind: DataFrameKind.PtyOutput,
        payload: new Uint8Array([1, 2]),
        ptyOffset: 12n,
      });
      const liveSecond = deliveryEnvelope("live", 2n, 179n, {
        eventSeq: 7n,
        kind: DataFrameKind.PtyOutput,
        payload: new Uint8Array([3]),
        ptyOffset: 14n,
      });
      const recoveryFirst = deliveryEnvelope("recovery", 1n, 90n, {
        eventSeq: 3n,
        kind: DataFrameKind.PtyOutput,
        payload: new Uint8Array([4, 5]),
        ptyOffset: 4n,
      });
      expect(
        transaction(durable, () =>
          store.enqueueValidatedLaneDelivery(recordFixture.recoveryId, liveFirst, 210),
        ),
      ).toMatchObject({ changed: true, ok: true });
      expect(
        transaction(durable, () =>
          store.enqueueValidatedLaneDelivery(recordFixture.recoveryId, recoveryFirst, 211),
        ),
      ).toMatchObject({ changed: true, ok: true });
      expect(store.deliveryUsage(recordFixture.recoveryId)).toEqual({
        encodedBytes: liveFirst.byteLength + recoveryFirst.byteLength,
        records: 2,
      });
      expect(
        transaction(durable, () =>
          store.enqueueValidatedLaneDelivery(recordFixture.recoveryId, liveSecond, 212),
        ),
      ).toEqual({ ok: false, reason: "delivery-capacity" });
    });

    const literalRecordFixture = await createSession();
    await runInDurableObject(sessionStub(literalRecordFixture.sessionId), (_instance, durable) => {
      seedClient(durable, literalRecordFixture, "v3");
      installAuthority(durable, committed);
      const store = recoveryStore(durable);
      transaction(durable, () => store.beginPreparing(beginInput(literalRecordFixture)));
      transaction(durable, () => store.installFence(installInput(literalRecordFixture)));
      for (const [lane, authority] of [
        ["live", committed],
        ["recovery", base],
      ] as const) {
        durable.storage.sql.exec(
          `WITH RECURSIVE seq(value) AS (
             VALUES(1) UNION ALL SELECT value + 1 FROM seq WHERE value < 512
           )
           INSERT INTO recovery_delivery_record
             (recovery_id, lane, delivery_ordinal, cumulative_encoded_bytes,
              encoded_bytes, authority_cursor_after_json, state)
           SELECT ?, ?, CAST(value AS TEXT), CAST(value AS TEXT), 1, ?, 'queued' FROM seq`,
          literalRecordFixture.recoveryId,
          lane,
          JSON.stringify(authority),
        );
      }
      expect(store.deliveryUsage(literalRecordFixture.recoveryId)).toEqual({
        encodedBytes: 1024,
        records: 1024,
      });
      installAuthority(durable, { sessionEpoch: "7", eventSeq: "6", nextPtyOffset: "14" });
      expect(
        transaction(durable, () =>
          store.enqueueValidatedLaneDelivery(
            literalRecordFixture.recoveryId,
            deliveryEnvelope("live", 513n, 602n, {
              eventSeq: 6n,
              kind: DataFrameKind.PtyOutput,
              payload: new Uint8Array([1, 2]),
              ptyOffset: 12n,
            }),
            210,
          ),
        ),
      ).toEqual({ ok: false, reason: "delivery-capacity" });
    });

    const literalByteCap = 2 * 1024 * 1024;
    const capPlusOneFixture = await createSession();
    await runInDurableObject(sessionStub(capPlusOneFixture.sessionId), (_instance, durable) => {
      seedClient(durable, capPlusOneFixture, "v3");
      installAuthority(durable, committed);
      const store = recoveryStore(durable);
      transaction(durable, () => store.beginPreparing(beginInput(capPlusOneFixture)));
      transaction(durable, () => store.installFence(installInput(capPlusOneFixture)));
      const payload = new Uint8Array(literalByteCap + 1 - 88);
      installAuthority(durable, {
        sessionEpoch: "7",
        eventSeq: "6",
        nextPtyOffset: (12 + payload.byteLength).toString(),
      });
      const envelope = deliveryEnvelope("live", 1n, BigInt(literalByteCap + 1), {
        eventSeq: 6n,
        kind: DataFrameKind.PtyOutput,
        payload,
        ptyOffset: 12n,
      });
      expect(
        transaction(durable, () =>
          store.enqueueValidatedLaneDelivery(capPlusOneFixture.recoveryId, envelope, 210),
        ),
      ).toEqual({ ok: false, reason: "delivery-capacity" });
    });

    const exactCapFixture = await createSession();
    await runInDurableObject(sessionStub(exactCapFixture.sessionId), (_instance, durable) => {
      seedClient(durable, exactCapFixture, "v3");
      installAuthority(durable, committed);
      const store = recoveryStore(durable);
      transaction(durable, () => store.beginPreparing(beginInput(exactCapFixture)));
      transaction(durable, () => store.installFence(installInput(exactCapFixture)));
      const payload = new Uint8Array(literalByteCap - 88);
      installAuthority(durable, {
        sessionEpoch: "7",
        eventSeq: "6",
        nextPtyOffset: (12 + payload.byteLength).toString(),
      });
      const envelope = deliveryEnvelope("live", 1n, BigInt(literalByteCap), {
        eventSeq: 6n,
        kind: DataFrameKind.PtyOutput,
        payload,
        ptyOffset: 12n,
      });
      expect(
        transaction(durable, () =>
          store.enqueueValidatedLaneDelivery(exactCapFixture.recoveryId, envelope, 210),
        ),
      ).toMatchObject({ changed: true, ok: true });
      expect(store.deliveryUsage(exactCapFixture.recoveryId)).toEqual({
        encodedBytes: literalByteCap,
        records: 1,
      });
    });
  });

  it("orders decimal ordinals numerically, rejects a missing sent prefix, and admits MAX_U64", async () => {
    const fixture = await createSession();
    await runInDurableObject(sessionStub(fixture.sessionId), (_instance, durable) => {
      seedClient(durable, fixture, "v3");
      installAuthority(durable, committed);
      const store = recoveryStore(durable);
      transaction(durable, () => store.beginPreparing(beginInput(fixture)));
      transaction(durable, () => store.installFence(installInput(fixture)));
      durable.storage.sql.exec(
        `UPDATE recovery_delivery_lane
         SET sent_delivery_ordinal = '8', sent_cumulative_encoded_bytes = '800',
             sent_authority_cursor_json = ?, received_delivery_ordinal = '8',
             received_cumulative_encoded_bytes = '800', received_authority_cursor_json = ?
         WHERE recovery_id = ? AND lane = 'live'`,
        JSON.stringify(committed),
        JSON.stringify(committed),
        fixture.recoveryId,
      );
      installAuthority(durable, { sessionEpoch: "7", eventSeq: "7", nextPtyOffset: "15" });
      const ninth = deliveryEnvelope("live", 9n, 890n, {
        eventSeq: 6n,
        kind: DataFrameKind.PtyOutput,
        payload: new Uint8Array([1, 2]),
        ptyOffset: 12n,
      });
      const tenth = deliveryEnvelope("live", 10n, 979n, {
        eventSeq: 7n,
        kind: DataFrameKind.PtyOutput,
        payload: new Uint8Array([3]),
        ptyOffset: 14n,
      });
      for (const [index, encoded] of [ninth, tenth].entries()) {
        const queued = transaction(durable, () =>
          store.enqueueValidatedLaneDelivery(fixture.recoveryId, encoded, 210 + index),
        );
        if (!queued.ok) throw new Error(queued.reason);
        expect(
          transaction(durable, () => store.beginLaneDeliverySend(queued.record, 212 + index)),
        ).toEqual({
          changed: true,
          ok: true,
        });
        expect(
          transaction(durable, () => store.confirmLaneDeliverySend(queued.record, 214 + index)),
        ).toEqual({ changed: true, ok: true });
      }
      expect(store.deliveryRecords(fixture.recoveryId).map((row) => row.delivery_ordinal)).toEqual([
        "9",
        "10",
      ]);

      durable.storage.sql.exec(
        `DELETE FROM recovery_delivery_record
         WHERE recovery_id = ? AND lane = 'live' AND delivery_ordinal = '9'`,
        fixture.recoveryId,
      );
      expect(
        transaction(durable, () =>
          store.markLaneReceived(
            {
              receipt: {
                type: "delivery-received",
                deliveryGeneration: "1",
                lane: "live",
                contiguousDeliveryOrdinal: "10",
                cumulativeEncodedBytes: "979",
              },
              recoveryId: fixture.recoveryId,
            },
            220,
          ),
        ),
      ).toEqual({ ok: false, reason: "invalid-progress" });

      durable.storage.sql.exec(
        "DELETE FROM recovery_delivery_record WHERE recovery_id = ?",
        fixture.recoveryId,
      );
      const beforeMax = ((1n << 64n) - 2n).toString();
      durable.storage.sql.exec(
        `UPDATE recovery_delivery_lane
         SET sent_delivery_ordinal = ?, sent_cumulative_encoded_bytes = '0',
             sent_authority_cursor_json = ?, received_delivery_ordinal = ?,
             received_cumulative_encoded_bytes = '0', received_authority_cursor_json = ?
         WHERE recovery_id = ? AND lane = 'live'`,
        beforeMax,
        JSON.stringify(committed),
        beforeMax,
        JSON.stringify(committed),
        fixture.recoveryId,
      );
      installAuthority(durable, { sessionEpoch: "7", eventSeq: "6", nextPtyOffset: "14" });
      const atMax = deliveryEnvelope("live", (1n << 64n) - 1n, 90n, {
        eventSeq: 6n,
        kind: DataFrameKind.PtyOutput,
        payload: new Uint8Array([1, 2]),
        ptyOffset: 12n,
      });
      expect(
        transaction(durable, () =>
          store.enqueueValidatedLaneDelivery(fixture.recoveryId, atMax, 221),
        ),
      ).toMatchObject({ changed: true, ok: true });
      expect(store.deliveryRecords(fixture.recoveryId)[0]?.delivery_ordinal).toBe(
        "18446744073709551615",
      );
      expect(() =>
        durable.storage.sql.exec(
          `INSERT INTO recovery_delivery_record
            (recovery_id, lane, delivery_ordinal, cumulative_encoded_bytes,
             encoded_bytes, authority_cursor_after_json, state)
           VALUES (?, 'recovery', '18446744073709551616', '1', 1, ?, 'queued')`,
          fixture.recoveryId,
          JSON.stringify(base),
        ),
      ).toThrow();
    });
  });

  it.each([
    { complete: false, phase: "queued", reset: "unsafe", sendsHostReset: true },
    { complete: true, phase: "queued", reset: "unsafe", sendsHostReset: false },
    { complete: true, phase: "sent", reset: "undeliverable", sendsHostReset: false },
    { complete: true, phase: "empty", reset: "undeliverable", sendsHostReset: false },
  ] as const)(
    "fences $phase delivery ownership for complete=$complete without an invalid Host reset",
    async ({ complete, phase, reset, sendsHostReset }) => {
      const fixture = await createSession();
      await runInDurableObject(sessionStub(fixture.sessionId), (_instance, durable) => {
        seedClient(durable, fixture, "v3");
        installAuthority(durable, committed);
        const store = recoveryStore(durable);
        transaction(durable, () => store.beginPreparing(beginInput(fixture)));
        transaction(durable, () => store.installFence(installInput(fixture)));
        if (complete) {
          durable.storage.sql.exec(
            "UPDATE recovery_attempt SET state = 'complete' WHERE recovery_id = ?",
            fixture.recoveryId,
          );
        }
        installAuthority(durable, { sessionEpoch: "7", eventSeq: "6", nextPtyOffset: "14" });
        const queued =
          phase === "empty"
            ? undefined
            : transaction(durable, () =>
                store.enqueueValidatedLaneDelivery(
                  fixture.recoveryId,
                  deliveryEnvelope("live", 1n, 90n, {
                    eventSeq: 6n,
                    kind: DataFrameKind.PtyOutput,
                    payload: new Uint8Array([1, 2]),
                    ptyOffset: 12n,
                  }),
                  210,
                ),
              );
        if (queued !== undefined && !queued.ok) throw new Error(queued.reason);
        if (phase === "sent") {
          if (queued === undefined || !queued.ok) throw new Error("queued record missing");
          transaction(durable, () => store.beginLaneDeliverySend(queued.record, 211));
          transaction(durable, () => store.confirmLaneDeliverySend(queued.record, 212));
        }
        expect(store.deliveryRecoveryIdsRequiringReset()).toEqual(
          phase === "queued" ? [fixture.recoveryId] : [],
        );

        const result = transaction(durable, () =>
          reset === "unsafe"
            ? store.resetUnsafeDeliveryOutcome(fixture.recoveryId, 220)
            : store.resetUndeliverableDeliveryOwner(fixture.recoveryId, 220),
        );
        expect(result).toEqual({ changed: true, ok: true });
        expect(
          transaction(durable, () =>
            reset === "unsafe"
              ? store.resetUnsafeDeliveryOutcome(fixture.recoveryId, 221)
              : store.resetUndeliverableDeliveryOwner(fixture.recoveryId, 221),
          ),
        ).toEqual({ changed: false, ok: true });
        expect(store.attempt(fixture.recoveryId)).toMatchObject({
          reset_reason: reset === "unsafe" ? "ack-outcome-uncertain" : "generation-reset",
          state: "resetting",
        });
        expect(store.deliveryRecords(fixture.recoveryId)).toEqual([]);
        expect(store.lanes(fixture.recoveryId)).toEqual([]);
        expect(store.outbox()).toMatchObject(
          sendsHostReset ? [{ destination: "host", kind: "recovery-source-reset" }] : [],
        );

        if (sendsHostReset) {
          const outbox = store.outbox()[0];
          if (outbox === undefined) throw new Error("reset outbox missing");
          transaction(durable, () =>
            store.acknowledgeOutbox(fixture.recoveryId, outbox.kind, outbox.payload_json, 221),
          );
        }
        expect(store.pruneFencedTerminalAttempts()).toEqual([]);
        durable.storage.sql.exec(
          "UPDATE client_delivery SET delivery_generation = '2' WHERE client_id = ?",
          fixture.clientId,
        );
        expect(store.pruneFencedTerminalAttempts()).toEqual([fixture.recoveryId]);
      });
    },
  );

  it.each(["generation", "host"] as const)(
    "cascades the scalar ledger exactly once when the %s fence advances",
    async (fence) => {
      const fixture = await createSession();
      await runInDurableObject(sessionStub(fixture.sessionId), (_instance, durable) => {
        seedClient(durable, fixture, "v3");
        installAuthority(durable, committed);
        const store = recoveryStore(durable);
        transaction(durable, () => store.beginPreparing(beginInput(fixture)));
        transaction(durable, () => store.installFence(installInput(fixture)));
        installAuthority(durable, { sessionEpoch: "7", eventSeq: "6", nextPtyOffset: "14" });
        expect(
          transaction(durable, () =>
            store.enqueueValidatedLaneDelivery(
              fixture.recoveryId,
              deliveryEnvelope("live", 1n, 90n, {
                eventSeq: 6n,
                kind: DataFrameKind.PtyOutput,
                payload: new Uint8Array([1, 2]),
                ptyOffset: 12n,
              }),
              210,
            ),
          ),
        ).toMatchObject({ changed: true, ok: true });

        const fenced = transaction(durable, () => {
          if (fence === "generation") {
            durable.storage.sql.exec(
              "UPDATE client_delivery SET delivery_generation = '2' WHERE client_id = ?",
              fixture.clientId,
            );
            return store.fenceClientGeneration(fixture.clientId, "2", 220);
          }
          durable.storage.sql.exec("UPDATE session_state SET host_fence = '4' WHERE singleton = 1");
          return store.fenceHost("4", 220);
        });
        expect(fenced).toEqual([fixture.recoveryId]);
        expect(store.deliveryRecords(fixture.recoveryId)).toEqual([]);
        expect(store.lanes(fixture.recoveryId)).toEqual([]);
        expect(store.outbox()).toMatchObject([
          { destination: "host", kind: "recovery-source-reset" },
        ]);
      });
    },
  );

  it.each(["source-closed-first", "adopted-first"] as const)(
    "keeps lane receipt/apply independent and completes in %s order",
    async (order) => {
      const fixture = await createSession();
      await runInDurableObject(sessionStub(fixture.sessionId), (_instance, durable) => {
        seedClient(durable, fixture, "v3");
        installAuthority(durable, committed);
        const store = recoveryStore(durable);
        transaction(durable, () => store.beginPreparing(beginInput(fixture)));
        transaction(durable, () => store.installFence(installInput(fixture)));

        expect(
          transaction(durable, () =>
            sendValidatedLaneDelivery(
              store,
              fixture.recoveryId,
              deliveryEnvelope("recovery", 2n, 90n, {
                eventSeq: 3n,
                kind: DataFrameKind.PtyOutput,
                payload: new Uint8Array([1, 2]),
                ptyOffset: 4n,
              }),
              210,
            ),
          ),
        ).toEqual({ ok: false, reason: "invalid-progress" });

        const sent = [
          deliveryEnvelope("recovery", 1n, 90n, {
            eventSeq: 3n,
            kind: DataFrameKind.PtyOutput,
            payload: new Uint8Array([1, 2]),
            ptyOffset: 4n,
          }),
          deliveryEnvelope("recovery", 2n, 180n, {
            eventSeq: 4n,
            kind: DataFrameKind.PtyOutput,
            payload: new Uint8Array([3, 4]),
            ptyOffset: 6n,
          }),
          deliveryEnvelope("recovery", 3n, 272n, {
            eventSeq: 5n,
            kind: DataFrameKind.PtyOutput,
            payload: new Uint8Array([5, 6, 7, 8]),
            ptyOffset: 8n,
          }),
          deliveryEnvelope("recovery", 4n, 360n, {
            eventSeq: 5n,
            kind: DataFrameKind.ReplayCommit,
            payload: new Uint8Array(),
            ptyOffset: 12n,
          }),
        ];
        for (const [index, encoded] of sent.entries()) {
          expect(
            transaction(durable, () =>
              sendValidatedLaneDelivery(store, fixture.recoveryId, encoded, 220 + index),
            ),
          ).toEqual({ changed: true, ok: true });
          if (index < 3) {
            expect(
              transaction(durable, () =>
                store.markLaneReceived(
                  {
                    receipt: {
                      type: "delivery-received",
                      deliveryGeneration: "1",
                      lane: "recovery",
                      contiguousDeliveryOrdinal: (index + 1).toString(),
                      cumulativeEncodedBytes: ["90", "180", "272"][index]!,
                    },
                    recoveryId: fixture.recoveryId,
                  },
                  220 + index,
                ),
              ),
            ).toEqual({ changed: true, ok: true });
          }
        }
        expect(store.attempt(fixture.recoveryId)).toMatchObject({
          recovery_done_cumulative_encoded_bytes: "360",
          recovery_done_ordinal: "4",
          recovery_done_through_json: JSON.stringify(committed),
        });

        expect(
          transaction(durable, () =>
            store.markReplicaApplied(fixture.recoveryId, "1", committed, 231),
          ),
        ).toEqual({ changed: true, ok: true });
        expect(
          transaction(durable, () =>
            store.markLaneReceived(
              {
                receipt: {
                  type: "delivery-received",
                  deliveryGeneration: "1",
                  lane: "recovery",
                  contiguousDeliveryOrdinal: "3",
                  cumulativeEncodedBytes: "271",
                },
                recoveryId: fixture.recoveryId,
              },
              232,
            ),
          ),
        ).toEqual({ ok: false, reason: "invalid-progress" });
        expect(
          transaction(durable, () =>
            store.markLaneReceived(
              {
                receipt: {
                  type: "delivery-received",
                  deliveryGeneration: "1",
                  lane: "recovery",
                  contiguousDeliveryOrdinal: "4",
                  cumulativeEncodedBytes: "360",
                },
                recoveryId: fixture.recoveryId,
              },
              233,
            ),
          ),
        ).toEqual({ changed: true, ok: true });
        expect(
          transaction(durable, () =>
            store.markReplicaApplied(fixture.recoveryId, "1", committed, 234),
          ),
        ).toEqual({ changed: false, ok: true });
        expect(store.attempt(fixture.recoveryId)).toMatchObject({
          no_progress_deadline_at: 1_233,
          no_progress_timeout_ms: 1_000,
        });

        const adopted = {
          type: "recovery-adopted",
          recoveryId: fixture.recoveryId,
          deliveryGeneration: "1",
          replicaApplied: committed,
        } satisfies RecoveryAdopted;
        const closed = {
          type: "recovery-source-closed",
          recoveryId: fixture.recoveryId,
          connectionId: fixture.connectionId,
          streamId: 1,
          deliveryGeneration: "1",
          throughRecoveryOrdinal: "4",
          throughRecoveryCumulativeEncodedBytes: "360",
        } satisfies RecoveryV3HostSourceClosed;
        const first =
          order === "source-closed-first"
            ? () => store.markSourceClosed(closed, 240)
            : () => store.markAdopted(adopted, 240);
        const second =
          order === "source-closed-first"
            ? () => store.markAdopted(adopted, 241)
            : () => store.markSourceClosed(closed, 241);
        expect(transaction(durable, first)).toEqual({ changed: true, ok: true });
        expect(store.attempt(fixture.recoveryId)?.state).not.toBe("complete");
        expect(transaction(durable, second)).toEqual({ changed: true, ok: true });
        expect(store.attempt(fixture.recoveryId)?.state).toBe("complete");
        expect(transaction(durable, first)).toEqual({ changed: false, ok: true });
        expect(transaction(durable, second)).toEqual({ changed: false, ok: true });

        installAuthority(durable, { sessionEpoch: "7", eventSeq: "7", nextPtyOffset: "15" });
        const continuedLive = [
          {
            cumulativeEncodedBytes: "90",
            cursor: { sessionEpoch: "7", eventSeq: "6", nextPtyOffset: "14" },
            encoded: deliveryEnvelope("live", 1n, 90n, {
              eventSeq: 6n,
              kind: DataFrameKind.PtyOutput,
              payload: new Uint8Array([1, 2]),
              ptyOffset: 12n,
            }),
            ordinal: "1",
          },
          {
            cumulativeEncodedBytes: "179",
            cursor: { sessionEpoch: "7", eventSeq: "7", nextPtyOffset: "15" },
            encoded: deliveryEnvelope("live", 2n, 179n, {
              eventSeq: 7n,
              kind: DataFrameKind.PtyOutput,
              payload: new Uint8Array([3]),
              ptyOffset: 14n,
            }),
            ordinal: "2",
          },
        ] as const;
        for (const [index, live] of continuedLive.entries()) {
          const now = 20_000 + index * 3;
          expect(
            transaction(durable, () =>
              sendValidatedLaneDelivery(store, fixture.recoveryId, live.encoded, now),
            ),
          ).toEqual({ changed: true, ok: true });
          expect(
            transaction(durable, () =>
              store.markLaneReceived(
                {
                  receipt: {
                    type: "delivery-received",
                    deliveryGeneration: "1",
                    lane: "live",
                    contiguousDeliveryOrdinal: live.ordinal,
                    cumulativeEncodedBytes: live.cumulativeEncodedBytes,
                  },
                  recoveryId: fixture.recoveryId,
                },
                now + 1,
              ),
            ),
          ).toEqual({ changed: true, ok: true });
          expect(
            transaction(durable, () =>
              store.markReplicaApplied(fixture.recoveryId, "1", live.cursor, now + 2),
            ),
          ).toEqual({ changed: true, ok: true });
        }
        expect(store.attempt(fixture.recoveryId)).toMatchObject({
          hard_deadline_at: 10_000,
          replica_applied_json: JSON.stringify(continuedLive[1].cursor),
          state: "complete",
        });
        expect(store.lanes(fixture.recoveryId)[0]).toMatchObject({
          lane: "live",
          received_cumulative_encoded_bytes: "179",
          received_delivery_ordinal: "2",
          sent_cumulative_encoded_bytes: "179",
          sent_delivery_ordinal: "2",
        });

        for (const entry of store.outbox()) {
          expect(
            transaction(durable, () =>
              store.acknowledgeOutbox(fixture.recoveryId, entry.kind, entry.payload_json, 20_100),
            ),
          ).toEqual({ changed: true, ok: true });
        }
        expect(store.outbox()).toEqual([]);
        expect(transaction(durable, () => store.expireDeadlines(20_101))).toEqual([]);
        expect(transaction(durable, () => store.pruneFencedTerminalAttempts())).toEqual([]);
        expect(store.attempt(fixture.recoveryId)?.state).toBe("complete");
      });
    },
  );

  it("keeps a current complete attempt as the live-lane owner only", async () => {
    const fixture = await createSession();
    await runInDurableObject(sessionStub(fixture.sessionId), (_instance, durable) => {
      seedClient(durable, fixture, "v3");
      installAuthority(durable, committed);
      const store = recoveryStore(durable);
      transaction(durable, () => store.beginPreparing(beginInput(fixture)));
      transaction(durable, () => store.installFence(installInput(fixture)));
      durable.storage.sql.exec(
        `UPDATE recovery_attempt
         SET state = 'complete', recovery_done_through_json = ?,
             recovery_done_ordinal = '1', recovery_done_cumulative_encoded_bytes = '88',
             updated_at = 250
         WHERE recovery_id = ?`,
        JSON.stringify(committed),
        fixture.recoveryId,
      );
      durable.storage.sql.exec(
        `UPDATE recovery_delivery_lane
         SET sent_delivery_ordinal = '1', sent_cumulative_encoded_bytes = '88',
             sent_authority_cursor_json = ?, received_delivery_ordinal = '1',
             received_cumulative_encoded_bytes = '88', received_authority_cursor_json = ?
         WHERE recovery_id = ? AND lane = 'recovery'`,
        JSON.stringify(committed),
        JSON.stringify(committed),
        fixture.recoveryId,
      );
      installAuthority(durable, { sessionEpoch: "7", eventSeq: "6", nextPtyOffset: "14" });

      const live = deliveryEnvelope("live", 1n, 90n, {
        eventSeq: 6n,
        kind: DataFrameKind.PtyOutput,
        payload: new Uint8Array([1, 2]),
        ptyOffset: 12n,
      });
      expect(
        transaction(durable, () =>
          sendValidatedLaneDelivery(store, fixture.recoveryId, live, 20_000),
        ),
      ).toEqual({ changed: true, ok: true });
      expect(
        transaction(durable, () =>
          store.markLaneReceived(
            {
              receipt: {
                type: "delivery-received",
                deliveryGeneration: "1",
                lane: "live",
                contiguousDeliveryOrdinal: "1",
                cumulativeEncodedBytes: "90",
              },
              recoveryId: fixture.recoveryId,
            },
            20_001,
          ),
        ),
      ).toEqual({ changed: true, ok: true });
      expect(
        transaction(durable, () =>
          store.markReplicaApplied(
            fixture.recoveryId,
            "1",
            { sessionEpoch: "7", eventSeq: "6", nextPtyOffset: "14" },
            20_002,
          ),
        ),
      ).toEqual({ changed: true, ok: true });

      expect(
        transaction(durable, () =>
          sendValidatedLaneDelivery(
            store,
            fixture.recoveryId,
            deliveryEnvelope("recovery", 1n, 90n, {
              eventSeq: 3n,
              kind: DataFrameKind.PtyOutput,
              payload: new Uint8Array([1, 2]),
              ptyOffset: 4n,
            }),
            20_003,
          ),
        ),
      ).toEqual({ ok: false, reason: "state-conflict" });
      expect(
        transaction(durable, () =>
          store.markAdopted(
            {
              type: "recovery-adopted",
              recoveryId: fixture.recoveryId,
              deliveryGeneration: "1",
              replicaApplied: committed,
            },
            20_004,
          ),
        ),
      ).toEqual({ ok: false, reason: "state-conflict" });
      expect(
        transaction(durable, () =>
          store.markSourceClosed(
            {
              type: "recovery-source-closed",
              recoveryId: fixture.recoveryId,
              connectionId: fixture.connectionId,
              streamId: 1,
              deliveryGeneration: "1",
              throughRecoveryOrdinal: "1",
              throughRecoveryCumulativeEncodedBytes: "90",
            },
            20_005,
          ),
        ),
      ).toEqual({ ok: false, reason: "state-conflict" });
      expect(store.attempt(fixture.recoveryId)?.state).toBe("complete");
      expect(store.pruneFencedTerminalAttempts()).toEqual([]);
    });
  });

  it("rejects new progress at the hard deadline without breaking exact scalar retries", async () => {
    const fixture = await createSession();
    await runInDurableObject(sessionStub(fixture.sessionId), (_instance, durable) => {
      seedClient(durable, fixture, "v3");
      installAuthority(durable, committed);
      const store = recoveryStore(durable);
      transaction(durable, () => store.beginPreparing(beginInput(fixture, 1_000)));
      transaction(durable, () => store.installFence(installInput(fixture)));
      installAuthority(durable, { sessionEpoch: "7", eventSeq: "7", nextPtyOffset: "16" });

      const liveFirst = deliveryEnvelope("live", 1n, 90n, {
        eventSeq: 6n,
        kind: DataFrameKind.PtyOutput,
        payload: new Uint8Array([1, 2]),
        ptyOffset: 12n,
      });
      expect(
        transaction(durable, () =>
          sendValidatedLaneDelivery(store, fixture.recoveryId, liveFirst, 210),
        ),
      ).toEqual({ changed: true, ok: true });

      const recoverySent = [
        deliveryEnvelope("recovery", 1n, 90n, {
          eventSeq: 3n,
          kind: DataFrameKind.PtyOutput,
          payload: new Uint8Array([1, 2]),
          ptyOffset: 4n,
        }),
        deliveryEnvelope("recovery", 2n, 180n, {
          eventSeq: 4n,
          kind: DataFrameKind.PtyOutput,
          payload: new Uint8Array([3, 4]),
          ptyOffset: 6n,
        }),
        deliveryEnvelope("recovery", 3n, 272n, {
          eventSeq: 5n,
          kind: DataFrameKind.PtyOutput,
          payload: new Uint8Array([5, 6, 7, 8]),
          ptyOffset: 8n,
        }),
        deliveryEnvelope("recovery", 4n, 360n, {
          eventSeq: 5n,
          kind: DataFrameKind.ReplayCommit,
          payload: new Uint8Array(),
          ptyOffset: 12n,
        }),
      ];
      for (const [index, encoded] of recoverySent.entries()) {
        expect(
          transaction(durable, () =>
            sendValidatedLaneDelivery(store, fixture.recoveryId, encoded, 220 + index * 2),
          ),
        ).toEqual({ changed: true, ok: true });
        if (index < 3) {
          expect(
            transaction(durable, () =>
              store.markLaneReceived(
                {
                  receipt: {
                    type: "delivery-received",
                    deliveryGeneration: "1",
                    lane: "recovery",
                    contiguousDeliveryOrdinal: (index + 1).toString(),
                    cumulativeEncodedBytes: ["90", "180", "272"][index]!,
                  },
                  recoveryId: fixture.recoveryId,
                },
                221 + index * 2,
              ),
            ),
          ).toEqual({ changed: true, ok: true });
        }
      }
      const recoveryReceipt = {
        type: "delivery-received",
        deliveryGeneration: "1",
        lane: "recovery",
        contiguousDeliveryOrdinal: "4",
        cumulativeEncodedBytes: "360",
      } as const;
      expect(
        transaction(durable, () =>
          store.markLaneReceived({ receipt: recoveryReceipt, recoveryId: fixture.recoveryId }, 231),
        ),
      ).toEqual({ changed: true, ok: true });
      expect(
        transaction(durable, () =>
          store.markReplicaApplied(fixture.recoveryId, "1", committed, 232),
        ),
      ).toEqual({ changed: true, ok: true });

      const adopted = {
        type: "recovery-adopted",
        recoveryId: fixture.recoveryId,
        deliveryGeneration: "1",
        replicaApplied: committed,
      } satisfies RecoveryAdopted;
      const closed = {
        type: "recovery-source-closed",
        recoveryId: fixture.recoveryId,
        connectionId: fixture.connectionId,
        streamId: 1,
        deliveryGeneration: "1",
        throughRecoveryOrdinal: "4",
        throughRecoveryCumulativeEncodedBytes: "360",
      } satisfies RecoveryV3HostSourceClosed;
      const atDeadline = 1_000;

      expect(
        transaction(durable, () =>
          store.installFence({ ...installInput(fixture), now: atDeadline }),
        ),
      ).toEqual({ changed: false, ok: true });
      expect(
        transaction(durable, () =>
          store.markReplicaApplied(fixture.recoveryId, "1", committed, atDeadline),
        ),
      ).toEqual({ changed: false, ok: true });
      expect(
        transaction(durable, () =>
          store.markLaneReceived(
            { receipt: recoveryReceipt, recoveryId: fixture.recoveryId },
            atDeadline,
          ),
        ),
      ).toEqual({ changed: false, ok: true });

      const outboxBeforeExpiredProgress = store.outbox();
      const expiredResults = [
        transaction(durable, () =>
          sendValidatedLaneDelivery(
            store,
            fixture.recoveryId,
            deliveryEnvelope("live", 2n, 180n, {
              eventSeq: 7n,
              kind: DataFrameKind.PtyOutput,
              payload: new Uint8Array([3, 4]),
              ptyOffset: 14n,
            }),
            atDeadline,
          ),
        ),
        transaction(durable, () =>
          store.markLaneReceived(
            {
              receipt: {
                type: "delivery-received",
                deliveryGeneration: "1",
                lane: "live",
                contiguousDeliveryOrdinal: "1",
                cumulativeEncodedBytes: "90",
              },
              recoveryId: fixture.recoveryId,
            },
            atDeadline,
          ),
        ),
        transaction(durable, () =>
          store.markReplicaApplied(
            fixture.recoveryId,
            "1",
            { sessionEpoch: "7", eventSeq: "6", nextPtyOffset: "14" },
            atDeadline,
          ),
        ),
        transaction(durable, () => store.markAdopted(adopted, atDeadline)),
        transaction(durable, () => store.markSourceClosed(closed, atDeadline)),
        transaction(durable, () =>
          store.advanceRecoveryGrant(fixture.recoveryId, "1001", atDeadline),
        ),
      ];
      expect(expiredResults).toEqual(
        Array.from({ length: expiredResults.length }, () => ({
          ok: false,
          reason: "deadline-expired",
        })),
      );
      expect(store.attempt(fixture.recoveryId)).toMatchObject({
        adopted_json: null,
        granted_cumulative_encoded_bytes: "1000",
        source_closed_json: null,
        state: "installed",
      });
      expect(store.lanes(fixture.recoveryId)[0]).toMatchObject({
        lane: "live",
        received_delivery_ordinal: "0",
        sent_delivery_ordinal: "1",
      });
      expect(store.outbox()).toEqual(outboxBeforeExpiredProgress);
    });
  });

  it("treats the no-progress boundary as expired while retaining an exact grant retry", async () => {
    const fixture = await createSession();
    await runInDurableObject(sessionStub(fixture.sessionId), (_instance, durable) => {
      seedClient(durable, fixture, "v3");
      installAuthority(durable, committed);
      const store = recoveryStore(durable);
      transaction(durable, () =>
        store.beginPreparing({
          ...beginInput(fixture),
          hardDeadlineAt: 10_000,
          noProgressTimeoutMs: 150,
        }),
      );
      transaction(durable, () => store.installFence(installInput(fixture)));
      expect(store.attempt(fixture.recoveryId)?.no_progress_deadline_at).toBe(350);
      expect(
        transaction(durable, () => store.advanceRecoveryGrant(fixture.recoveryId, "1001", 349)),
      ).toEqual({ changed: true, ok: true });
      expect(
        transaction(durable, () => store.advanceRecoveryGrant(fixture.recoveryId, "1001", 350)),
      ).toEqual({ changed: false, ok: true });
      expect(
        transaction(durable, () => store.advanceRecoveryGrant(fixture.recoveryId, "1002", 350)),
      ).toEqual({ ok: false, reason: "deadline-expired" });
      expect(store.attempt(fixture.recoveryId)?.granted_cumulative_encoded_bytes).toBe("1001");
    });
  });

  it.each(["generation", "host"] as const)(
    "retains a current-owner complete tombstone until the %s fence changes",
    async (changedFence) => {
      const fixture = await createSession();
      await runInDurableObject(sessionStub(fixture.sessionId), (_instance, durable) => {
        seedClient(durable, fixture, "v3");
        installAuthority(durable, committed);
        const store = recoveryStore(durable);
        transaction(durable, () => store.beginPreparing(beginInput(fixture)));
        transaction(durable, () => store.installFence(installInput(fixture)));

        // Completion transitions are covered above; isolate tombstone ownership here.
        durable.storage.sql.exec(
          "UPDATE recovery_attempt SET state = 'complete', updated_at = 250 WHERE recovery_id = ?",
          fixture.recoveryId,
        );
        for (const entry of store.outbox()) {
          transaction(durable, () =>
            store.acknowledgeOutbox(fixture.recoveryId, entry.kind, entry.payload_json, 251),
          );
        }
        expect(store.attempt(fixture.recoveryId)?.state).toBe("complete");
        expect(store.pruneFencedTerminalAttempts()).toEqual([]);

        const replacement: SessionFixture = {
          ...fixture,
          connectionId: `${fixture.connectionId}_replacement`,
          recoveryId: `${fixture.recoveryId}_replacement`,
        };
        expect(transaction(durable, () => store.beginPreparing(beginInput(replacement)))).toEqual({
          ok: false,
          reason: "generation-owned",
        });

        if (changedFence === "generation") {
          durable.storage.sql.exec(
            "UPDATE client_delivery SET delivery_generation = '2' WHERE client_id = ?",
            fixture.clientId,
          );
        } else {
          durable.storage.sql.exec("UPDATE session_state SET host_fence = '4' WHERE singleton = 1");
        }
        expect(store.pruneFencedTerminalAttempts()).toEqual([fixture.recoveryId]);
        expect(store.attempt(fixture.recoveryId)).toBeUndefined();
      });
    },
  );

  it("owns deadlines and snapshot pins and fences replaced hosts or removed clients", async () => {
    const fixture = await createSession();
    await runInDurableObject(sessionStub(fixture.sessionId), (_instance, durable) => {
      seedClient(durable, fixture, "v3");
      installAuthority(durable, committed);
      const store = recoveryStore(durable);
      const snapshotId = "snapshot_recovery_0001";
      expect(
        transaction(durable, () =>
          store.beginPreparing({
            ...beginInput(fixture),
            prepare: { ...prepareFor(fixture), source: { kind: "snapshot", snapshotId } },
          }),
        ),
      ).toEqual({ changed: true, ok: true });
      expect(store.nextDeadline()).toBe(1_100);
      expect([...store.pinnedSnapshotIds()]).toEqual([snapshotId]);

      transaction(durable, () => {
        durable.storage.sql.exec("UPDATE session_state SET host_fence = '4' WHERE singleton = 1");
        expect(store.advanceRecoveryGrant(fixture.recoveryId, "1", 199)).toEqual({
          ok: false,
          reason: "identity-mismatch",
        });
        expect(store.fenceHost("4", 200)).toEqual([fixture.recoveryId]);
      });
      expect(store.attempt(fixture.recoveryId)).toMatchObject({
        reset_reason: "pair-fenced",
        state: "resetting",
      });
      expect(store.pinnedSnapshotIds().size).toBe(0);
      expect(store.nextDeadline()).toBeUndefined();

      const other: SessionFixture = {
        ...fixture,
        clientId: `${fixture.clientId}_removed`,
        connectionId: `${fixture.connectionId}_removed`,
        recoveryId: `${fixture.recoveryId}_removed`,
      };
      seedClient(durable, other, "v3", "1", 2);
      expect(
        transaction(durable, () =>
          store.beginPreparing({
            ...beginInput(other),
            hostFence: "4",
            prepare: prepareFor(other, "1", 2),
          }),
        ),
      ).toEqual({ changed: true, ok: true });
      transaction(durable, () => {
        expect(store.fenceRemovedClient(other.clientId, 300)).toEqual([other.recoveryId]);
        durable.storage.sql.exec("DELETE FROM client_delivery WHERE client_id = ?", other.clientId);
      });
      expect(store.attempt(other.recoveryId)).toMatchObject({
        reset_reason: "pair-fenced",
        state: "resetting",
      });
      const removedReset = store.outbox().find((entry) => entry.recovery_id === other.recoveryId);
      if (removedReset === undefined) throw new Error("removed client reset outbox missing");
      transaction(durable, () =>
        store.acknowledgeOutbox(
          other.recoveryId,
          removedReset.kind,
          removedReset.payload_json,
          301,
        ),
      );
      expect(store.attempt(other.recoveryId)).toBeUndefined();
    });
  });

  it("rolls back a host fence when all reset outboxes cannot fit", async () => {
    const fixture = await createSession();
    await runInDurableObject(sessionStub(fixture.sessionId), (_instance, durable) => {
      const second: SessionFixture = {
        ...fixture,
        clientId: `${fixture.clientId}_second`,
        connectionId: `${fixture.connectionId}_second`,
        recoveryId: `${fixture.recoveryId}_second`,
      };
      seedClient(durable, fixture, "v3");
      seedClient(durable, second, "v3", "1", 2);
      installAuthority(durable, committed);
      const bounded = recoveryStore(durable, {
        maxAttempts: 2,
        maxDeliveryEncodedBytes: 2 * 1024 * 1024,
        maxDeliveryRecords: 1024,
        maxOutboxEntries: 1,
      });
      for (const [candidate, streamId] of [
        [fixture, 1],
        [second, 2],
      ] as const) {
        expect(
          transaction(durable, () =>
            bounded.beginPreparing({
              ...beginInput(candidate),
              prepare: prepareFor(candidate, "1", streamId),
            }),
          ),
        ).toEqual({ changed: true, ok: true });
        const entry = bounded.outbox()[0];
        if (entry === undefined) throw new Error("prepare outbox missing");
        transaction(durable, () =>
          bounded.acknowledgeOutbox(candidate.recoveryId, entry.kind, entry.payload_json, 101),
        );
      }
      expect(bounded.outbox()).toHaveLength(0);
      expect(() =>
        transaction(durable, () => {
          durable.storage.sql.exec("UPDATE session_state SET host_fence = '4' WHERE singleton = 1");
          bounded.fenceHost("4", 200);
        }),
      ).toThrow("recovery reset outbox capacity exceeded");
      expect(new RelayStore(durable.storage.sql).session()?.host_fence).toBe("3");
      expect(bounded.attempt(fixture.recoveryId)?.state).toBe("preparing");
      expect(bounded.attempt(second.recoveryId)?.state).toBe("preparing");
      expect(bounded.outbox()).toHaveLength(0);
    });
  });

  it("fences replaced generations and reports only deadline resets that fit the outbox", async () => {
    const fixture = await createSession();
    await runInDurableObject(sessionStub(fixture.sessionId), (_instance, durable) => {
      seedClient(durable, fixture, "v3");
      installAuthority(durable, committed);
      const bounded = recoveryStore(durable, {
        maxAttempts: 2,
        maxDeliveryEncodedBytes: 2 * 1024 * 1024,
        maxDeliveryRecords: 1024,
        maxOutboxEntries: 1,
      });
      expect(
        transaction(durable, () =>
          bounded.beginPreparing({
            ...beginInput(fixture, 1_000),
            noProgressTimeoutMs: 10,
            now: 1,
          }),
        ),
      ).toEqual({ changed: true, ok: true });
      const prepare = bounded.outbox()[0];
      if (prepare === undefined) throw new Error("prepare outbox missing");
      expect(
        transaction(durable, () =>
          bounded.acknowledgeOutbox(fixture.recoveryId, prepare.kind, prepare.payload_json, 2),
        ),
      ).toEqual({ changed: true, ok: true });

      const other: SessionFixture = {
        ...fixture,
        clientId: `${fixture.clientId}_other`,
        connectionId: `${fixture.connectionId}_other`,
        recoveryId: `${fixture.recoveryId}_other`,
      };
      seedClient(durable, other, "v3", "1", 2);
      expect(
        transaction(durable, () =>
          bounded.beginPreparing({
            ...beginInput(other, 1_000),
            noProgressTimeoutMs: 10,
            now: 3,
            prepare: prepareFor(other, "1", 2),
          }),
        ),
      ).toEqual({ changed: true, ok: true });
      expect(bounded.outbox()).toHaveLength(1);

      expect(transaction(durable, () => bounded.expireDeadlines(12))).toEqual([]);
      expect(bounded.attempt(fixture.recoveryId)?.state).toBe("preparing");
      expect(bounded.outbox()).toHaveLength(1);
      expect(bounded.nextDeadline()).toBe(11);

      const otherPrepare = bounded.outbox()[0];
      if (otherPrepare === undefined) throw new Error("other prepare outbox missing");
      transaction(durable, () =>
        bounded.acknowledgeOutbox(
          other.recoveryId,
          otherPrepare.kind,
          otherPrepare.payload_json,
          13,
        ),
      );
      expect(transaction(durable, () => bounded.expireDeadlines(13))).toEqual([fixture.recoveryId]);
      expect(bounded.attempt(fixture.recoveryId)).toMatchObject({
        reset_reason: "deadline",
        state: "resetting",
      });
      expect(bounded.outbox()).toMatchObject([
        { kind: "recovery-source-reset", recovery_id: fixture.recoveryId },
      ]);

      const deadlineReset = bounded.outbox()[0];
      if (deadlineReset === undefined) throw new Error("deadline reset outbox missing");
      transaction(durable, () =>
        bounded.acknowledgeOutbox(
          fixture.recoveryId,
          deadlineReset.kind,
          deadlineReset.payload_json,
          14,
        ),
      );

      durable.storage.sql.exec(
        "UPDATE client_delivery SET delivery_generation = '2' WHERE client_id = ?",
        other.clientId,
      );
      expect(bounded.advanceRecoveryGrant(other.recoveryId, "1", 14)).toEqual({
        ok: false,
        reason: "identity-mismatch",
      });
      expect(
        transaction(durable, () => bounded.fenceClientGeneration(other.clientId, "2", 15)),
      ).toEqual([other.recoveryId]);
    });
  });
});
