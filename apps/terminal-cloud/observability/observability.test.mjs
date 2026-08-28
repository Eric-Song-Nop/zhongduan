import { readFile } from "node:fs/promises";

import { describe, expect, it, vi } from "vitest";

import { main } from "../../../scripts/cloud-telemetry-observability.mjs";
import {
  createCloudflareObservabilityClient,
  extractAggregateResult,
  provisionSavedQueries,
} from "./cloudflare-observability.mjs";
import {
  DEFAULT_MANIFEST_URL,
  requiredKeyTypes,
  savedQuerySpec,
  loadManifest,
  validateManifest,
} from "./manifest.mjs";
import { loadFixture, runArrivalSmoke } from "./operations.mjs";

function jsonResponse(result, init = {}) {
  return new Response(JSON.stringify({ success: true, result }), {
    status: 200,
    headers: { "content-type": "application/json" },
    ...init,
  });
}

function aggregateResult(alias, options = {}) {
  return {
    statistics: {
      bytes_read: 10,
      elapsed: 0.001,
      rows_read: 1,
      abr_level: options.abrLevel ?? 1,
    },
    calculations: [
      {
        calculation: `sum(${alias})`,
        alias,
        aggregates: [
          {
            count: 1,
            interval: 1,
            sampleInterval: options.sampleInterval ?? 1,
            value: options.value ?? 64,
            groups: options.groups ?? [],
            ...options.extraAggregate,
          },
        ],
        series: [],
      },
    ],
  };
}

async function rawManifest() {
  return JSON.parse(await readFile(DEFAULT_MANIFEST_URL, "utf8"));
}

function memoryStream() {
  let value = "";
  return {
    stream: { write: (chunk) => (value += String(chunk)) },
    read: () => value,
  };
}

describe("Cloud Workers Observability manifest", () => {
  it("expands strict base filters and covers every Cloud fact family", async () => {
    const manifest = await loadManifest();
    expect(manifest.queries).toHaveLength(19);
    const names = manifest.queries.flatMap((query) =>
      query.parameters.filters
        .filter((filter) => filter.key === "name")
        .map((filter) => filter.value),
    );
    expect(new Set(names)).toEqual(
      new Set([
        "cloud.relay.queue",
        "cloud.input.forward",
        "cloud.input.ack-forward",
        "cloud.data.fanout",
        "cloud.writer.lease",
        "cloud.recovery.barrier",
        "cloud.recovery.transition",
      ]),
    );
    for (const query of manifest.queries) {
      expect(query.parameters.filters.slice(0, 4)).toEqual([
        {
          kind: "filter",
          key: "$metadata.service",
          operation: "eq",
          type: "string",
          value: "zhongduan-terminal-cloud",
        },
        {
          kind: "filter",
          key: "$metadata.type",
          operation: "eq",
          type: "string",
          value: "cf-worker-log",
        },
        {
          kind: "filter",
          key: "type",
          operation: "eq",
          type: "string",
          value: "zhongduan.telemetry",
        },
        {
          kind: "filter",
          key: "runtime",
          operation: "eq",
          type: "string",
          value: "cloud-do",
        },
      ]);
    }
  });

  it("statically proves every percentile query selects one producer weight", async () => {
    const manifest = await loadManifest();
    const percentiles = manifest.queries.filter(
      (query) => query.parameters.calculations[0].operator === "p95",
    );
    expect(percentiles).toHaveLength(10);
    for (const query of percentiles) {
      expect(query.parameters.filters.filter((filter) => filter.key === "sampleWeight")).toEqual([
        expect.objectContaining({ operation: "eq", type: "number", value: expect.any(Number) }),
      ]);
    }
    const input = manifest.queries.find((query) => query.id === "cloud-input-forward-p95");
    expect(input.parameters.filters).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ key: "outcome", value: "send-returned" }),
        expect.objectContaining({ key: "sampleWeight", value: 16 }),
      ]),
    );
    const canonical = manifest.queries.find((query) => query.id === "cloud-canonical-fanout-p95");
    expect(canonical.parameters.filters).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ key: "outcome", value: "completed" }),
        expect.objectContaining({ key: "sampleWeight", value: 64 }),
      ]),
    );
  });

  it("rejects mixed-weight percentiles", async () => {
    const manifest = await rawManifest();
    const input = manifest.queries.find((query) => query.id === "cloud-input-forward-p95");
    input.filters = input.filters.filter((filter) => filter.key !== "sampleWeight");
    expect(() => validateManifest(manifest)).toThrow(/percentile queries must statically select/u);
  });

  it("keeps platform keys exact and rejects guessed payload prefixes", async () => {
    const manifest = await rawManifest();
    manifest.directKeyTypes["source.name"] = "string";
    expect(() => validateManifest(manifest)).toThrow(/outside this exact namespace/u);
    delete manifest.directKeyTypes["source.name"];
    manifest.platformKeyTypes["$metadata.service.name"] = "string";
    expect(() => validateManifest(manifest)).toThrow(/outside this exact namespace/u);
  });
});

describe("Cloudflare Observability API boundary", () => {
  it("verifies exact key/type pairs before sending a direct-key query", async () => {
    const manifest = await loadManifest();
    const query = manifest.queries.find((candidate) => candidate.id === "cloud-v2-arrival");
    const calls = [];
    const fetchImpl = vi.fn(async (url, init) => {
      calls.push({ url: String(url), init });
      if (String(url).endsWith("/telemetry/keys")) {
        return jsonResponse(
          [...requiredKeyTypes(query)].map(([key, type]) => ({ key, type, lastSeenAt: 1 })),
        );
      }
      return jsonResponse(aggregateResult(query.metricAlias));
    });
    const client = createCloudflareObservabilityClient({
      accountId: "account",
      token: "private-token",
      fetchImpl,
      baseUrl: "https://api.example.test/client/v4",
    });
    const result = await client.runVerifiedQuery(query, { from: 1, to: 2 });
    expect(result.aggregates[0].value).toBe(64);
    expect(calls.map((call) => call.url)).toEqual([
      "https://api.example.test/client/v4/accounts/account/workers/observability/telemetry/keys",
      "https://api.example.test/client/v4/accounts/account/workers/observability/telemetry/query",
    ]);
    const queryBody = JSON.parse(calls[1].init.body);
    expect(queryBody.parameters.filters.map((filter) => filter.key)).toContain("type");
    expect(queryBody.parameters.filters.map((filter) => filter.key)).not.toContain("source.type");
    expect(queryBody).toMatchObject({ chartType: "aggregate", dry: true, ignoreSeries: true });
  });

  it("does not accept a prefixed lookalike key or run the query after key mismatch", async () => {
    const manifest = await loadManifest();
    const query = manifest.queries.find((candidate) => candidate.id === "cloud-v2-arrival");
    const fetchImpl = vi.fn(async () =>
      jsonResponse([{ key: "source.type", type: "string", lastSeenAt: 1 }]),
    );
    const client = createCloudflareObservabilityClient({
      accountId: "account",
      token: "token",
      fetchImpl,
    });
    await expect(client.runVerifiedQuery(query, { from: 1, to: 2 })).rejects.toThrow(
      /required direct telemetry key unavailable/u,
    );
    expect(fetchImpl).toHaveBeenCalledTimes(1);
  });

  it("marks platform sampling approximate without multiplying the aggregate again", async () => {
    const manifest = await loadManifest();
    const query = manifest.queries.find((candidate) => candidate.id === "cloud-v2-arrival");
    const extracted = extractAggregateResult(
      aggregateResult(query.metricAlias, { value: 64, sampleInterval: 8, abrLevel: 2 }),
      query,
    );
    expect(extracted).toMatchObject({
      approximate: true,
      approximationReasons: ["abr-level", "sample-interval"],
      abrLevel: 2,
    });
    expect(extracted.aggregates[0].value).toBe(64);
  });

  it("rejects unallowlisted aggregate fields instead of exposing raw result data", async () => {
    const manifest = await loadManifest();
    const query = manifest.queries.find((candidate) => candidate.id === "cloud-v2-arrival");
    expect(() =>
      extractAggregateResult(
        aggregateResult(query.metricAlias, { extraAggregate: { source: "private raw source" } }),
        query,
      ),
    ).toThrow(/invalid aggregate response shape/u);
  });

  it("sanitizes HTTP failures without printing token or API error body", async () => {
    const privateToken = "private-observability-token";
    const privateBody = "private API error body";
    const client = createCloudflareObservabilityClient({
      accountId: "account",
      token: privateToken,
      fetchImpl: async () =>
        new Response(privateBody, { status: 403, headers: { "content-type": "text/plain" } }),
    });
    const manifest = await loadManifest();
    const query = manifest.queries[0];
    const error = await client
      .runVerifiedQuery(query, { from: 1, to: 2 })
      .catch((caught) => caught);
    expect(error.message).toContain("status 403");
    expect(error.message).not.toContain(privateToken);
    expect(error.message).not.toContain(privateBody);
  });
});

describe("report gates and saved query provisioning", () => {
  it("blocks but never hard-gates an approximate arrival smoke", async () => {
    const manifest = await loadManifest();
    const client = {
      discoverKeys: vi.fn(
        async () =>
          new Map([
            ...Object.entries(manifest.platformKeyTypes),
            ...Object.entries(manifest.directKeyTypes),
          ]),
      ),
      runVerifiedQuery: vi.fn(async (query) => ({
        queryId: query.id,
        metricAlias: query.metricAlias,
        unit: query.unit,
        approximate: true,
        approximationReasons: ["sample-interval"],
        abrLevel: 1,
        aggregates: [{ value: 0, count: 0, interval: 1, sampleInterval: 2, groups: [] }],
      })),
    };
    const smoke = await runArrivalSmoke(client, manifest, { from: 1, to: 2 });
    expect(smoke).toMatchObject({
      kind: "cloud-query-smoke",
      performanceGate: false,
      approximate: true,
      blocked: true,
      failed: false,
    });
    expect(smoke.checks[0]).toMatchObject({ status: "approximate", hardGateApplied: false });
  });

  it("preflights every manifest key before the arrival query", async () => {
    const manifest = await loadManifest();
    const discovered = new Map([
      ...Object.entries(manifest.platformKeyTypes),
      ...Object.entries(manifest.directKeyTypes),
    ]);
    discovered.delete("observedLeaseOutcomeMs");
    const client = {
      discoverKeys: vi.fn(async () => discovered),
      runVerifiedQuery: vi.fn(),
    };
    await expect(runArrivalSmoke(client, manifest, { from: 1, to: 2 })).rejects.toThrow(
      /required manifest telemetry key unavailable/u,
    );
    expect(client.runVerifiedQuery).not.toHaveBeenCalled();
  });

  it("requires apply, no-ops exact content, creates missing queries, and fails closed on drift", async () => {
    const manifest = await loadManifest();
    await expect(provisionSavedQueries({}, manifest)).rejects.toThrow(/explicit --apply/u);

    const first = manifest.queries[0];
    const client = {
      listSavedQueries: vi.fn(async () => [{ id: "saved", ...savedQuerySpec(first) }]),
      createSavedQuery: vi.fn(async (spec) => ({ id: "created", ...spec })),
    };
    const results = await provisionSavedQueries(
      client,
      { ...manifest, queries: manifest.queries.slice(0, 2) },
      { apply: true },
    );
    expect(results).toEqual([
      { queryId: first.id, action: "no-op" },
      { queryId: manifest.queries[1].id, action: "created" },
    ]);
    expect(client.createSavedQuery).toHaveBeenCalledTimes(1);

    const second = manifest.queries[1];
    const drifted = {
      ...savedQuerySpec(second),
      id: "saved",
      description: "drifted externally",
    };
    const driftClient = {
      listSavedQueries: vi.fn(async () => [drifted]),
      createSavedQuery: vi.fn(),
    };
    await expect(
      provisionSavedQueries(
        driftClient,
        { ...manifest, queries: [first, second] },
        { apply: true },
      ),
    ).rejects.toThrow(/drift detected/u);
    expect(driftClient.createSavedQuery).not.toHaveBeenCalled();
  });
});

describe("observability CLI", () => {
  it("validates the manifest without credentials or network access", async () => {
    const stdout = memoryStream();
    const stderr = memoryStream();
    const environment = new Proxy(
      {},
      {
        get() {
          throw new Error("credentials must not be read");
        },
      },
    );
    const code = await main(["validate"], {
      stdout: stdout.stream,
      stderr: stderr.stream,
      environment,
      fetchImpl: () => {
        throw new Error("network must not be used");
      },
    });
    expect(code).toBe(0);
    expect(stderr.read()).toBe("");
    expect(JSON.parse(stdout.read())).toMatchObject({
      status: "valid",
      queryCount: 19,
      fixture: { status: "synthetic-sanitized", requiresStagingReplacement: true },
    });
  });

  it("exposes the synthetic fixture replacement gate", async () => {
    const stdout = memoryStream();
    const stderr = memoryStream();
    expect(await main(["fixture-gate"], { stdout: stdout.stream, stderr: stderr.stream })).toBe(2);
    expect(JSON.parse(stdout.read())).toMatchObject({ requiresStagingReplacement: true });
    expect(stderr.read()).toBe("");
    expect((await loadFixture()).status).toBe("synthetic-sanitized");
  });

  it("calls the online smoke an arrival smoke rather than a performance canary", async () => {
    const stdout = memoryStream();
    const stderr = memoryStream();
    const fixture = await loadFixture();
    const fetchImpl = vi.fn(async (url) =>
      String(url).endsWith("/telemetry/keys")
        ? jsonResponse(fixture.keysResult)
        : jsonResponse(fixture.queryResult),
    );
    const code = await main(["arrival-smoke", "--from", "1", "--to", "2"], {
      stdout: stdout.stream,
      stderr: stderr.stream,
      environment: {
        CLOUDFLARE_OBSERVABILITY_TOKEN: "token",
        CLOUDFLARE_OBSERVABILITY_ACCOUNT_ID: "account",
      },
      fetchImpl,
      baseUrl: "https://api.example.test/client/v4",
    });
    expect(code).toBe(0);
    expect(stderr.read()).toBe("");
    expect(JSON.parse(stdout.read())).toMatchObject({
      kind: "cloud-query-smoke",
      performanceGate: false,
      blocked: false,
      failed: false,
    });
  });

  it("returns a non-zero blocked result for approximate arrival data", async () => {
    const stdout = memoryStream();
    const stderr = memoryStream();
    const fixture = await loadFixture();
    const approximate = structuredClone(fixture.queryResult);
    approximate.calculations[0].aggregates[0].sampleInterval = 2;
    const fetchImpl = vi.fn(async (url) =>
      String(url).endsWith("/telemetry/keys")
        ? jsonResponse(fixture.keysResult)
        : jsonResponse(approximate),
    );
    const code = await main(["arrival-smoke", "--from", "1", "--to", "2"], {
      stdout: stdout.stream,
      stderr: stderr.stream,
      environment: {
        CLOUDFLARE_OBSERVABILITY_TOKEN: "token",
        CLOUDFLARE_OBSERVABILITY_ACCOUNT_ID: "account",
      },
      fetchImpl,
      baseUrl: "https://api.example.test/client/v4",
    });
    expect(code).toBe(2);
    expect(stderr.read()).toBe("");
    expect(JSON.parse(stdout.read())).toMatchObject({
      performanceGate: false,
      approximate: true,
      blocked: true,
      failed: false,
      checks: [{ status: "approximate", hardGateApplied: false }],
    });
  });
});
