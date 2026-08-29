import {
  CapabilityResponseSchema,
  ConnectionSetResponseSchema,
  CreateSessionResponseSchema,
  RELAY_CAPABILITIES_HEADER,
  RelayCapability,
  SNAPSHOT_MEDIA_TYPE,
  SnapshotHeader,
  SnapshotMetadataSchema,
  SnapshotUploadResponseSchema,
  confirmedRelayCapabilities,
  type CapabilityResponse,
  type ConnectionSetRequest,
  type ConnectionSetResponse,
  type CreateSessionResponse,
  type SnapshotMetadata,
  type SnapshotUploadResponse,
} from "@zhongduan/protocol";

interface Schema<T> {
  parse(input: unknown): T;
}

const HOST_RELAY_CAPABILITIES = [
  RelayCapability.capabilityNegotiationV1,
  RelayCapability.authorityDataV2,
  RelayCapability.deliveryBarrierOutcomeV1,
] as const;

export type CloudFetch = typeof fetch;

export class CloudApiError extends Error {
  constructor(
    readonly status: number,
    readonly errorCode: string,
  ) {
    super(`cloud API request failed (${status}: ${errorCode})`);
    this.name = "CloudApiError";
  }
}

export class CloudTransportError extends Error {
  constructor(cause: unknown) {
    super("cloud API transport failed", { cause });
    this.name = "CloudTransportError";
  }
}

export interface CloudApiClientOptions {
  fetch?: CloudFetch;
}

export class CloudApiClient {
  readonly #basePath: string;
  readonly #baseUrl: URL;
  readonly #fetch: CloudFetch;

  constructor(baseUrl: string | URL, options: CloudApiClientOptions = {}) {
    const parsed = new URL(baseUrl);
    if (
      (parsed.protocol !== "https:" &&
        (parsed.protocol !== "http:" || !isLoopbackHost(parsed.hostname))) ||
      parsed.username !== "" ||
      parsed.password !== "" ||
      parsed.search !== "" ||
      parsed.hash !== ""
    ) {
      throw new TypeError("cloud URL must be an HTTP(S) origin without credentials or query data");
    }
    this.#basePath = parsed.pathname.replace(/\/+$/u, "");
    parsed.pathname = this.#basePath === "" ? "/" : this.#basePath;
    this.#baseUrl = parsed;
    this.#fetch = options.fetch ?? fetch;
  }

  async createSession(
    bootstrapToken: string,
    sessionId: string,
    engineId: string,
    sessionEpoch: bigint,
    signal?: AbortSignal,
  ): Promise<CreateSessionResponse> {
    const created = await this.#postJson(
      "/api/v1/sessions",
      bootstrapToken,
      { sessionId, engineId, sessionEpoch: sessionEpoch.toString() },
      CreateSessionResponseSchema,
      signal,
    );
    if (
      created.sessionId !== sessionId ||
      created.engineId !== engineId ||
      BigInt(created.sessionEpoch) !== sessionEpoch
    ) {
      throw new CloudApiError(200, "session-identity-mismatch");
    }
    return created;
  }

  async createConnectionSet(
    sessionId: string,
    capability: string,
    request: ConnectionSetRequest = {},
    signal?: AbortSignal,
  ): Promise<ConnectionSetResponse> {
    const connection = await this.#postJson(
      `/api/v1/sessions/${encodeURIComponent(sessionId)}/connection-sets`,
      capability,
      request,
      ConnectionSetResponseSchema,
      signal,
      {
        [RELAY_CAPABILITIES_HEADER]: HOST_RELAY_CAPABILITIES.join(","),
      },
    );
    const confirmed = confirmedRelayCapabilities(connection, HOST_RELAY_CAPABILITIES);
    if (confirmed.length === 0) delete connection.negotiatedCapabilities;
    else connection.negotiatedCapabilities = confirmed;
    return connection;
  }

  refreshCapability(
    sessionId: string,
    capability: string,
    signal?: AbortSignal,
  ): Promise<CapabilityResponse> {
    return this.#postJson(
      `/api/v1/sessions/${encodeURIComponent(sessionId)}/capabilities/refresh`,
      capability,
      {},
      CapabilityResponseSchema,
      signal,
    );
  }

  reclaimHostCapability(
    sessionId: string,
    bootstrapToken: string,
    engineId: string,
    sessionEpoch: bigint,
    signal?: AbortSignal,
  ): Promise<CapabilityResponse> {
    return this.#postJson(
      `/api/v1/sessions/${encodeURIComponent(sessionId)}/capabilities/host/reclaim`,
      bootstrapToken,
      { engineId, sessionEpoch: sessionEpoch.toString() },
      CapabilityResponseSchema,
      signal,
    );
  }

  async uploadSnapshot(
    metadata: SnapshotMetadata,
    body: Uint8Array,
    capability: string,
    signal?: AbortSignal,
  ): Promise<SnapshotUploadResponse> {
    const expected = SnapshotMetadataSchema.parse(metadata);
    if (body.byteLength !== Number(expected.compressedLength)) {
      throw new RangeError("snapshot body length does not match its metadata");
    }
    if (!(body.buffer instanceof ArrayBuffer)) {
      throw new RangeError("snapshot upload body must use an ArrayBuffer");
    }
    const response = await this.#request(
      this.#url(
        `/api/v1/sessions/${encodeURIComponent(expected.sessionId)}/snapshots/${encodeURIComponent(expected.snapshotId)}`,
      ),
      {
        method: "PUT",
        redirect: "error",
        headers: {
          authorization: `Bearer ${capability}`,
          "cache-control": "no-store",
          "content-length": expected.compressedLength,
          "content-type": SNAPSHOT_MEDIA_TYPE,
          [SnapshotHeader.compression]: expected.compression,
          [SnapshotHeader.compressedLength]: expected.compressedLength,
          [SnapshotHeader.cutEventSeq]: expected.cutEventSeq,
          [SnapshotHeader.engineId]: expected.engineId,
          [SnapshotHeader.nextPtyOffset]: expected.nextPtyOffset,
          [SnapshotHeader.sessionEpoch]: expected.sessionEpoch,
          [SnapshotHeader.sha256]: expected.sha256,
          [SnapshotHeader.uncompressedLength]: expected.uncompressedLength,
        },
        body: body as Uint8Array<ArrayBuffer>,
        ...(signal === undefined ? {} : { signal }),
      },
    );
    if (response.ok && response.status !== 200 && response.status !== 201) {
      cancelResponseBody(response, "unexpected snapshot upload status");
      throw new CloudApiError(response.status, "invalid-response");
    }
    const uploaded = await responseJson(response, SnapshotUploadResponseSchema);
    if (!sameSnapshotMetadata(uploaded.snapshot, expected)) {
      throw new Error("cloud API finalized a different snapshot");
    }
    return uploaded;
  }

  webSocketUrl(sessionId: string, channel: "control" | "data", ticket: string): string {
    const url = this.#url(`/api/v1/sessions/${encodeURIComponent(sessionId)}/ws/${channel}`);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    url.searchParams.set("ticket", ticket);
    return url.href;
  }

  async #postJson<T>(
    pathname: string,
    bearer: string,
    body: object,
    schema: Schema<T>,
    signal?: AbortSignal,
    extraHeaders: Record<string, string> = {},
  ): Promise<T> {
    const response = await this.#request(this.#url(pathname), {
      method: "POST",
      redirect: "error",
      headers: {
        authorization: `Bearer ${bearer}`,
        "cache-control": "no-store",
        "content-type": "application/json",
        ...extraHeaders,
      },
      body: JSON.stringify(body),
      ...(signal === undefined ? {} : { signal }),
    });
    return responseJson(response, schema);
  }

  async #request(input: URL, init: RequestInit): Promise<Response> {
    try {
      return await this.#fetch(input, init);
    } catch (error) {
      if (init.signal?.aborted) throw init.signal.reason;
      throw new CloudTransportError(error);
    }
  }

  #url(pathname: string): URL {
    const url = new URL(this.#baseUrl);
    url.pathname = `${this.#basePath}${pathname}`;
    return url;
  }
}

function cancelResponseBody(response: Response, reason: string): void {
  if (response.body === null || response.body.locked) return;
  void response.body.cancel(reason).catch(() => undefined);
}

function isLoopbackHost(hostname: string): boolean {
  return hostname === "localhost" || hostname === "127.0.0.1" || hostname === "[::1]";
}

async function responseJson<T>(response: Response, schema: Schema<T>): Promise<T> {
  let body: unknown;
  try {
    body = await response.json();
  } catch {
    throw new CloudApiError(response.status, response.ok ? "invalid-response" : "unknown-error");
  }
  if (!response.ok) {
    const errorCode =
      typeof body === "object" && body !== null && "error" in body && typeof body.error === "string"
        ? body.error
        : "unknown-error";
    throw new CloudApiError(response.status, errorCode);
  }
  try {
    return schema.parse(body);
  } catch {
    throw new CloudApiError(response.status, "invalid-response");
  }
}

function sameSnapshotMetadata(left: SnapshotMetadata, right: SnapshotMetadata): boolean {
  return (
    left.sessionId === right.sessionId &&
    left.snapshotId === right.snapshotId &&
    left.engineId === right.engineId &&
    left.sessionEpoch === right.sessionEpoch &&
    left.cutEventSeq === right.cutEventSeq &&
    left.nextPtyOffset === right.nextPtyOffset &&
    left.compression === right.compression &&
    left.compressedLength === right.compressedLength &&
    left.uncompressedLength === right.uncompressedLength &&
    left.sha256 === right.sha256
  );
}
