import { MAX_U64 } from "@zhongduan/protocol";

import { randomId, sha256Hex } from "./auth";
import { readSocketAttachment, writeSocketAttachment, type SocketAttachment } from "./relay-socket";
import type { RelayStore } from "./relay-store";

const WRITER_LEASE_MS = 30_000;

export interface AcquiredWriterLease {
  expiresAt: number;
  fence: string;
  token: string;
}

export type LiveWriterAttachment = SocketAttachment & {
  clientId: string;
  leaseExpiresAt: number;
  leaseFence: string;
};

export interface WriterAuthorityOptions {
  activeBrowserControl: (clientId: string) => WebSocket | undefined;
  closeDisplacedWriters: (connectionId: string, fence: string) => void;
  now?: () => number;
  sql: SqlStorage;
  store: RelayStore;
}

/** Owns the connection-scoped writer lease, fence, and live-attachment proof. */
export class WriterAuthority {
  readonly #activeBrowserControl: (clientId: string) => WebSocket | undefined;
  readonly #closeDisplacedWriters: (connectionId: string, fence: string) => void;
  readonly #now: () => number;
  readonly #sql: SqlStorage;
  readonly #store: RelayStore;

  constructor(options: WriterAuthorityOptions) {
    this.#activeBrowserControl = options.activeBrowserControl;
    this.#closeDisplacedWriters = options.closeDisplacedWriters;
    this.#now = options.now ?? Date.now;
    this.#sql = options.sql;
    this.#store = options.store;
  }

  async acquire(
    clientId: string,
    connectionId: string,
    now: number,
  ): Promise<AcquiredWriterLease | undefined> {
    const existing = this.#store.writerLease();
    if (existing !== undefined && existing.expires_at > now) return undefined;
    const token = randomId(32);
    const digest = await sha256Hex(token);
    const current = this.#store.writerLease();
    if (current !== undefined && current.expires_at > this.#now()) return undefined;
    const currentFence = BigInt(current?.fence ?? "0");
    if (currentFence >= MAX_U64) throw new Error("writer lease fence space exhausted");
    const fence = (currentFence + 1n).toString();
    const expiresAt = this.#now() + WRITER_LEASE_MS;
    this.#sql.exec(
      `INSERT INTO writer_lease
        (singleton, client_id, connection_id, lease_digest, fence, expires_at)
       VALUES (1, ?, ?, ?, ?, ?)
       ON CONFLICT(singleton) DO UPDATE SET
         client_id = excluded.client_id,
         connection_id = excluded.connection_id,
         lease_digest = excluded.lease_digest,
         fence = excluded.fence,
         expires_at = excluded.expires_at`,
      clientId,
      connectionId,
      digest,
      fence,
      expiresAt,
    );
    this.#closeDisplacedWriters(connectionId, fence);
    return { expiresAt, fence, token };
  }

  async renew(
    webSocket: WebSocket,
    attachment: SocketAttachment,
    token: string,
  ): Promise<number | undefined> {
    const initial = this.liveAttachment(webSocket, attachment, this.#now());
    if (initial === undefined || token.length > 256) return undefined;
    const digest = await sha256Hex(token);
    const currentAttachment = this.liveAttachment(webSocket, initial, this.#now());
    if (currentAttachment === undefined) return undefined;
    const lease = this.#store.writerLease();
    if (
      lease === undefined ||
      lease.client_id !== currentAttachment.clientId ||
      lease.connection_id !== currentAttachment.connectionId ||
      lease.fence !== currentAttachment.leaseFence ||
      lease.lease_digest !== digest ||
      lease.expires_at <= this.#now()
    ) {
      return undefined;
    }
    const expiresAt = this.#now() + WRITER_LEASE_MS;
    this.#sql.exec(
      `UPDATE writer_lease SET expires_at = ?
       WHERE singleton = 1 AND client_id = ? AND connection_id = ?
         AND fence = ? AND lease_digest = ? AND expires_at > ?`,
      expiresAt,
      currentAttachment.clientId,
      currentAttachment.connectionId,
      currentAttachment.leaseFence,
      digest,
      this.#now(),
    );
    const renewed = this.#store.writerLease();
    if (
      renewed?.client_id !== currentAttachment.clientId ||
      renewed.connection_id !== currentAttachment.connectionId ||
      renewed.fence !== currentAttachment.leaseFence ||
      renewed.lease_digest !== digest ||
      renewed.expires_at !== expiresAt
    ) {
      return undefined;
    }
    writeSocketAttachment(webSocket, { ...currentAttachment, leaseExpiresAt: expiresAt });
    return expiresAt;
  }

  release(clientId: string, fence: string | null, connectionId: string): void {
    if (fence === null) return;
    this.#sql.exec(
      `UPDATE writer_lease SET expires_at = 0
       WHERE singleton = 1 AND client_id = ? AND connection_id = ? AND fence = ?`,
      clientId,
      connectionId,
      fence,
    );
  }

  liveAttachment(
    webSocket: WebSocket,
    expected: SocketAttachment,
    now: number,
  ): LiveWriterAttachment | undefined {
    const current = readSocketAttachment(webSocket);
    if (
      webSocket.readyState !== WebSocket.OPEN ||
      current?.peer !== "browser" ||
      current.channel !== "control" ||
      current.role !== "writer" ||
      current.controlState !== "active" ||
      current.clientId === null ||
      current.clientId !== expected.clientId ||
      current.connectionId !== expected.connectionId ||
      current.connectionSetId !== expected.connectionSetId ||
      current.leaseFence === null ||
      current.leaseFence !== expected.leaseFence ||
      current.leaseExpiresAt === null ||
      current.leaseExpiresAt <= now ||
      this.#activeBrowserControl(current.clientId) !== webSocket
    ) {
      return undefined;
    }
    return current as LiveWriterAttachment;
  }
}
