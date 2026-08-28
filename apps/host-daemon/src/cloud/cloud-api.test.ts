import { SNAPSHOT_MEDIA_TYPE, SnapshotHeader, type SnapshotMetadata } from "@zhongduan/protocol";
import { describe, expect, it, vi } from "vitest";

import { CloudApiClient, CloudApiError, CloudTransportError } from "./cloud-api";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

const metadata: SnapshotMetadata = {
  sessionId: "session_AAAAAAAAA",
  snapshotId: "snapshot_AAAAAAAA",
  engineId: "ghostty/test",
  sessionEpoch: "7",
  cutEventSeq: "11",
  nextPtyOffset: "13",
  compression: "zstd",
  compressedLength: "4",
  uncompressedLength: "20",
  sha256: "a".repeat(64),
};

describe("CloudApiClient", () => {
  it("creates a session with exact bootstrap identity and strict response parsing", async () => {
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
      jsonResponse({
        sessionId: "session_AAAAAAAAA",
        engineId: "ghostty/test",
        sessionEpoch: "7",
        hostCapability: "host-cap",
        hostCapabilityExpiresAt: 2_000,
        writerCapability: "writer-cap",
        writerCapabilityExpiresAt: 1_500,
        observerCapability: "observer-cap",
        observerCapabilityExpiresAt: 1_500,
      }),
    );
    const api = new CloudApiClient("https://cloud.example/base/", {
      fetch: fetchMock as unknown as typeof fetch,
    });

    await expect(
      api.createSession("bootstrap-secret", "session_AAAAAAAAA", "ghostty/test", 7n),
    ).resolves.toMatchObject({
      sessionId: "session_AAAAAAAAA",
      hostCapability: "host-cap",
    });

    const [input, init] = fetchMock.mock.calls[0]!;
    expect(input).toBeInstanceOf(URL);
    expect((input as URL).href).toBe("https://cloud.example/base/api/v1/sessions");
    expect(init?.method).toBe("POST");
    expect(new Headers(init?.headers).get("authorization")).toBe("Bearer bootstrap-secret");
    expect(typeof init?.body).toBe("string");
    expect(JSON.parse(init?.body as string)).toEqual({
      sessionId: "session_AAAAAAAAA",
      engineId: "ghostty/test",
      sessionEpoch: "7",
    });
  });

  it("uploads the immutable snapshot body with the complete metadata contract", async () => {
    const fetchMock = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
      jsonResponse({ created: true, snapshot: metadata }),
    );
    const api = new CloudApiClient("https://cloud.example", {
      fetch: fetchMock as unknown as typeof fetch,
    });
    const body = Uint8Array.of(0, 0x28, 0xb5, 0x2f, 0xfd, 0).subarray(1, 5);

    await expect(api.uploadSnapshot(metadata, body, "host-cap")).resolves.toEqual({
      created: true,
      snapshot: metadata,
    });

    const [input, init] = fetchMock.mock.calls[0]!;
    const headers = new Headers(init?.headers);
    expect(input).toBeInstanceOf(URL);
    expect((input as URL).href).toBe(
      "https://cloud.example/api/v1/sessions/session_AAAAAAAAA/snapshots/snapshot_AAAAAAAA",
    );
    expect(init?.method).toBe("PUT");
    expect(headers.get("content-type")).toBe(SNAPSHOT_MEDIA_TYPE);
    expect(headers.get("content-length")).toBe("4");
    expect(headers.get(SnapshotHeader.compression)).toBe("zstd");
    expect(headers.get(SnapshotHeader.cutEventSeq)).toBe("11");
    expect(headers.get(SnapshotHeader.sha256)).toBe("a".repeat(64));
    expect(init?.body).toBe(body);
  });

  it("rejects and cancels unexpected successful snapshot statuses", async () => {
    const cancel = vi.fn();
    const responseBody = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(JSON.stringify({ created: true })));
      },
      cancel,
    });
    const api = new CloudApiClient("https://cloud.example", {
      fetch: vi.fn(
        async () =>
          new Response(responseBody, {
            status: 202,
            headers: { "content-type": "application/json" },
          }),
      ) as unknown as typeof fetch,
    });

    await expect(
      api.uploadSnapshot(metadata, Uint8Array.of(1, 2, 3, 4), "host-cap"),
    ).rejects.toMatchObject({
      name: "CloudApiError",
      status: 202,
      errorCode: "invalid-response",
    } satisfies Partial<CloudApiError>);
    await vi.waitFor(() => expect(cancel).toHaveBeenCalledOnce());
  });

  it("distinguishes transport failure from API and local contract errors", async () => {
    const cause = new TypeError("network down");
    const api = new CloudApiClient("https://cloud.example", {
      fetch: vi.fn(async () => {
        throw cause;
      }) as unknown as typeof fetch,
    });

    await expect(api.refreshCapability("session_AAAAAAAAA", "host-cap")).rejects.toMatchObject({
      name: "CloudTransportError",
      cause,
    } satisfies Partial<CloudTransportError>);
    await expect(api.uploadSnapshot(metadata, Uint8Array.of(1), "host-cap")).rejects.toThrow(
      /body length/,
    );
  });

  it("builds channel-specific ticket URLs without leaking tickets elsewhere", () => {
    const api = new CloudApiClient("https://cloud.example/base");

    expect(api.webSocketUrl("session_AAAAAAAAA", "control", "ticket-secret")).toBe(
      "wss://cloud.example/base/api/v1/sessions/session_AAAAAAAAA/ws/control?ticket=ticket-secret",
    );
  });

  it("requires HTTPS outside explicit loopback development and rejects redirects", async () => {
    expect(() => new CloudApiClient("http://example.com")).toThrow(/cloud URL/);
    expect(() => new CloudApiClient("http://localhost:8787")).not.toThrow();
    expect(() => new CloudApiClient("http://127.0.0.1:8787")).not.toThrow();

    const fetchMock = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      expect(init?.redirect).toBe("error");
      return jsonResponse({ error: "redirect-blocked" }, 502);
    });
    const api = new CloudApiClient("https://cloud.example", {
      fetch: fetchMock as unknown as typeof fetch,
    });
    await expect(api.refreshCapability("session_AAAAAAAAA", "secret-cap")).rejects.toMatchObject({
      status: 502,
    });
    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it("does not forward a bearer token across an actual HTTP redirect", async () => {
    let redirectedRequests = 0;
    const destination = createServer((_request, response) => {
      redirectedRequests += 1;
      response.end();
    });
    const destinationUrl = await listen(destination);
    const redirect = createServer((_request, response) => {
      response.writeHead(307, { location: `${destinationUrl}/stolen` });
      response.end();
    });
    const redirectUrl = await listen(redirect);

    try {
      const api = new CloudApiClient(redirectUrl);
      await expect(
        api.refreshCapability("session_AAAAAAAAA", "secret-capability"),
      ).rejects.toMatchObject({ name: "CloudTransportError" });
      expect(redirectedRequests).toBe(0);
    } finally {
      await Promise.all([close(redirect), close(destination)]);
    }
  });
});

async function listen(server: ReturnType<typeof createServer>): Promise<string> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("expected TCP address");
  return `http://127.0.0.1:${address.port}`;
}

function close(server: ReturnType<typeof createServer>): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error !== undefined) reject(error);
      else resolve();
    });
  });
}
import { createServer } from "node:http";
