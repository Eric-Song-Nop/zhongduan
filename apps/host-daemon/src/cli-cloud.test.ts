import { createServer, type Server } from "node:http";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { runCloud } from "./cli";

const servers: Server[] = [];
const temporaryDirectories: string[] = [];

async function listen(server: Server): Promise<string> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  servers.push(server);
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("expected TCP address");
  return `http://127.0.0.1:${address.port}`;
}

async function sessionInfoPath(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "zhongduan-cloud-cli-"));
  temporaryDirectories.push(directory);
  return join(directory, "session.json");
}

async function bootstrapTokenPath(token = "bootstrap-secret"): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "zhongduan-cloud-token-"));
  temporaryDirectories.push(directory);
  const path = join(directory, "bootstrap-token");
  await writeFile(path, `${token}\n`, { encoding: "utf8", mode: 0o600 });
  return path;
}

async function within<T>(operation: Promise<T>, milliseconds = 3_000): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<T>((_resolve, reject) => {
        timeout = setTimeout(() => reject(new Error("operation timed out")), milliseconds);
      }),
    ]);
  } finally {
    if (timeout !== undefined) clearTimeout(timeout);
  }
}

afterEach(async () => {
  for (const server of servers.splice(0)) {
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
  await Promise.all(
    temporaryDirectories.splice(0).map((directory) => rm(directory, { recursive: true })),
  );
});

describe("cloud CLI startup lifecycle", () => {
  it("rejects bootstrap secrets passed through argv", async () => {
    const infoPath = await sessionInfoPath();

    await expect(
      runCloud([
        "--url",
        "http://127.0.0.1:1",
        "--bootstrap-token",
        "visible-in-process-list",
        "--session-info-file",
        infoPath,
      ]),
    ).resolves.toBe(2);
  });

  it("aborts a pending createSession request when the child exits first", async () => {
    let requestStarted = false;
    let resolveResponseClosed!: () => void;
    const responseClosed = new Promise<void>((resolve) => {
      resolveResponseClosed = resolve;
    });
    const server = createServer((_request, response) => {
      requestStarted = true;
      response.once("close", resolveResponseClosed);
    });
    const url = await listen(server);
    const infoPath = await sessionInfoPath();
    const tokenPath = await bootstrapTokenPath();

    const result = await within(
      runCloud([
        "--url",
        url,
        "--bootstrap-token-file",
        tokenPath,
        "--session-info-file",
        infoPath,
        "--",
        "/bin/sh",
        "-c",
        "sleep 0.1",
      ]),
    );

    expect(result).toBe(0);
    expect(requestStarted).toBe(true);
    await within(responseClosed);
  });

  it("bounds a 201 create response whose JSON body never completes", async () => {
    let requestStarted = false;
    let resolveResponseClosed!: () => void;
    const responseClosed = new Promise<void>((resolve) => {
      resolveResponseClosed = resolve;
    });
    const server = createServer((_request, response) => {
      requestStarted = true;
      response.once("close", resolveResponseClosed);
      response.writeHead(201, { "content-type": "application/json" });
      response.write('{"sessionId":');
    });
    const url = await listen(server);
    const infoPath = await sessionInfoPath();
    const tokenPath = await bootstrapTokenPath();

    const result = await within(
      runCloud([
        "--url",
        url,
        "--bootstrap-token-file",
        tokenPath,
        "--session-info-file",
        infoPath,
        "--",
        "/bin/sh",
        "-c",
        "sleep 0.1",
      ]),
    );

    expect(result).toBe(0);
    expect(requestStarted).toBe(true);
    await within(responseClosed);
  });

  it("re-reads a rotated bootstrap token file while cloud creation is degraded", async () => {
    const authorizations: Array<string | undefined> = [];
    let connectionSetRequests = 0;
    let snapshotUploadRequests = 0;
    const tokenPath = await bootstrapTokenPath("expired-bootstrap");
    const server = createServer(async (request, response) => {
      if (request.url === "/api/v1/sessions") {
        authorizations.push(request.headers.authorization);
        if (request.headers.authorization === "Bearer expired-bootstrap") {
          await writeFile(tokenPath, "rotated-bootstrap\n", { encoding: "utf8", mode: 0o600 });
          response.statusCode = 401;
          response.setHeader("content-type", "application/json");
          response.end(JSON.stringify({ error: "invalid-bootstrap" }));
          return;
        }
        const body = await new Promise<string>((resolve) => {
          let value = "";
          request.setEncoding("utf8");
          request.on("data", (chunk) => {
            value += chunk;
          });
          request.on("end", () => resolve(value));
        });
        const identity = JSON.parse(body) as {
          engineId: string;
          sessionEpoch: string;
          sessionId: string;
        };
        response.setHeader("content-type", "application/json");
        response.end(
          JSON.stringify({
            sessionId: identity.sessionId,
            engineId: identity.engineId,
            sessionEpoch: identity.sessionEpoch,
            hostCapability: "host-capability",
            hostCapabilityExpiresAt: Math.floor(Date.now() / 1_000) + 3_600,
            writerCapability: "writer-capability",
            writerCapabilityExpiresAt: Math.floor(Date.now() / 1_000) + 3_600,
            observerCapability: "observer-capability",
            observerCapabilityExpiresAt: Math.floor(Date.now() / 1_000) + 3_600,
          }),
        );
        return;
      }
      if (request.url?.endsWith("/connection-sets")) connectionSetRequests += 1;
      if (request.method === "PUT" && request.url?.includes("/snapshots/")) {
        snapshotUploadRequests += 1;
      }
    });
    const url = await listen(server);
    const infoPath = await sessionInfoPath();

    const result = await within(
      runCloud([
        "--url",
        url,
        "--bootstrap-token-file",
        tokenPath,
        "--session-info-file",
        infoPath,
        "--",
        "/bin/sh",
        "-c",
        "sleep 1.3",
      ]),
    );

    expect(result).toBe(0);
    expect(authorizations).toEqual(["Bearer expired-bootstrap", "Bearer rotated-bootstrap"]);
    expect(connectionSetRequests).toBe(1);
    expect(snapshotUploadRequests).toBe(1);
  });

  it("reuses one caller-generated session id after the create response is lost", async () => {
    const requestedSessionIds: string[] = [];
    let createRequests = 0;
    const server = createServer(async (request, response) => {
      if (request.url === "/api/v1/sessions") {
        createRequests += 1;
        const body = await new Promise<string>((resolve) => {
          let value = "";
          request.setEncoding("utf8");
          request.on("data", (chunk) => {
            value += chunk;
          });
          request.on("end", () => resolve(value));
        });
        const identity = JSON.parse(body) as {
          engineId: string;
          sessionEpoch: string;
          sessionId: string;
        };
        requestedSessionIds.push(identity.sessionId);
        if (createRequests === 1) {
          response.destroy();
          return;
        }
        response.setHeader("content-type", "application/json");
        response.end(
          JSON.stringify({
            sessionId: identity.sessionId,
            engineId: identity.engineId,
            sessionEpoch: identity.sessionEpoch,
            hostCapability: "host-capability",
            hostCapabilityExpiresAt: Math.floor(Date.now() / 1_000) + 3_600,
            writerCapability: "writer-capability",
            writerCapabilityExpiresAt: Math.floor(Date.now() / 1_000) + 3_600,
            observerCapability: "observer-capability",
            observerCapabilityExpiresAt: Math.floor(Date.now() / 1_000) + 3_600,
          }),
        );
      }
    });
    const url = await listen(server);
    const infoPath = await sessionInfoPath();
    const tokenPath = await bootstrapTokenPath();

    const result = await within(
      runCloud([
        "--url",
        url,
        "--bootstrap-token-file",
        tokenPath,
        "--session-info-file",
        infoPath,
        "--",
        "/bin/sh",
        "-c",
        "sleep 1.3",
      ]),
    );

    expect(result).toBe(0);
    expect(requestedSessionIds).toHaveLength(2);
    expect(new Set(requestedSessionIds).size).toBe(1);
  });

  it("stops a black-holed first relay handshake when the child exits", async () => {
    let connectionSetRequests = 0;
    const upgradeSockets = new Set<import("node:stream").Duplex>();
    const server = createServer(async (request, response) => {
      const body = await new Promise<string>((resolve) => {
        let value = "";
        request.setEncoding("utf8");
        request.on("data", (chunk) => {
          value += chunk;
        });
        request.on("end", () => resolve(value));
      });
      if (request.url === "/api/v1/sessions") {
        const identity = JSON.parse(body) as {
          engineId: string;
          sessionEpoch: string;
          sessionId: string;
        };
        response.setHeader("content-type", "application/json");
        response.end(
          JSON.stringify({
            sessionId: identity.sessionId,
            engineId: identity.engineId,
            sessionEpoch: identity.sessionEpoch,
            hostCapability: "host-capability",
            hostCapabilityExpiresAt: Math.floor(Date.now() / 1_000) + 3_600,
            writerCapability: "writer-capability",
            writerCapabilityExpiresAt: Math.floor(Date.now() / 1_000) + 3_600,
            observerCapability: "observer-capability",
            observerCapabilityExpiresAt: Math.floor(Date.now() / 1_000) + 3_600,
          }),
        );
        return;
      }
      if (request.url?.endsWith("/connection-sets")) {
        connectionSetRequests += 1;
        response.setHeader("content-type", "application/json");
        response.end(
          JSON.stringify({
            connectionSetId: "connection_set_AA",
            connectionId: "connection_AAAAA",
            clientId: null,
            streamId: 0,
            deliveryGeneration: "0",
            expiresAt: Math.floor(Date.now() / 1_000) + 30,
            controlTicket: "control_ticket_A",
            dataTicket: "data_ticket_AAAA",
          }),
        );
        return;
      }
      response.statusCode = 404;
      response.end();
    });
    server.on("upgrade", (_request, socket) => {
      upgradeSockets.add(socket);
      socket.once("close", () => upgradeSockets.delete(socket));
    });
    const url = await listen(server);
    const infoPath = await sessionInfoPath();
    const tokenPath = await bootstrapTokenPath();

    const result = await within(
      runCloud([
        "--url",
        url,
        "--bootstrap-token-file",
        tokenPath,
        "--session-info-file",
        infoPath,
        "--",
        "/bin/sh",
        "-c",
        "sleep 0.2",
      ]),
    );
    for (const socket of upgradeSockets) socket.destroy();

    expect(result).toBe(0);
    expect(connectionSetRequests).toBe(1);
  });
});
