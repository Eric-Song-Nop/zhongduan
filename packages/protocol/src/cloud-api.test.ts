import { describe, expect, it } from "vitest";

import {
  CapabilityMintRequestSchema,
  CapabilityRefreshRequestSchema,
  CapabilityResponseSchema,
  ConnectionSetRequestSchema,
  ConnectionSetResponseSchema,
  BrowserConnectionSetResponseSchema,
  CreateSessionRequestSchema,
  HostCapabilityReclaimRequestSchema,
} from "./cloud-api";

describe("Cloud HTTP contracts", () => {
  it("requires a stable caller-generated session resource id", () => {
    expect(
      CreateSessionRequestSchema.parse({
        sessionId: "session_AAAAAAAAAAAA",
        engineId: "engine",
        sessionEpoch: "7",
      }),
    ).toMatchObject({ sessionId: "session_AAAAAAAAAAAA" });
    expect(() =>
      CreateSessionRequestSchema.parse({ engineId: "engine", sessionEpoch: "7" }),
    ).toThrow();
  });

  it("requires one complete connection-set delivery identity", () => {
    expect(ConnectionSetRequestSchema.parse({ clientId: "client_id_AAAAAAAAAAA" })).toEqual({
      clientId: "client_id_AAAAAAAAAAA",
    });
    expect(() => ConnectionSetRequestSchema.parse({ clientId: "short" })).toThrow();

    const response = {
      connectionSetId: "connection_set_AAAAA",
      connectionId: "connection_id_AAAAAA",
      clientId: "client_id_AAAAAAAAAAA",
      streamId: 3,
      deliveryGeneration: "2",
      expiresAt: 1_800_000_000_000,
      controlTicket: "control_ticket_AAAAAA",
      dataTicket: "data_ticket_AAAAAAAAA",
    } as const;
    expect(BrowserConnectionSetResponseSchema.parse(response)).toEqual(response);
    expect(ConnectionSetResponseSchema.parse(response)).toEqual(response);
    expect(() => BrowserConnectionSetResponseSchema.parse({ ...response, streamId: 0 })).toThrow();
    expect(() =>
      ConnectionSetResponseSchema.parse({ ...response, deliveryGeneration: "0" }),
    ).toThrow();
    expect(
      ConnectionSetResponseSchema.parse({
        ...response,
        clientId: null,
        streamId: 0,
        deliveryGeneration: "0",
      }),
    ).toMatchObject({ clientId: null, streamId: 0, deliveryGeneration: "0" });
  });

  it("keeps capability roles and reclaim identity strict", () => {
    expect(CapabilityMintRequestSchema.parse({ role: "writer" })).toEqual({ role: "writer" });
    expect(CapabilityRefreshRequestSchema.parse({})).toEqual({});
    expect(() => CapabilityMintRequestSchema.parse({ role: "host" })).toThrow();
    expect(() => CapabilityRefreshRequestSchema.parse({ rotateRole: "host" })).toThrow();
    expect(
      HostCapabilityReclaimRequestSchema.parse({ engineId: "engine", sessionEpoch: "7" }),
    ).toEqual({ engineId: "engine", sessionEpoch: "7" });
  });

  it("bounds issued authorization capabilities", () => {
    expect(
      CapabilityResponseSchema.parse({
        capability: "zcap1.payload.signature",
        expiresAt: 1_800_000_000,
        role: "observer",
      }),
    ).toMatchObject({ role: "observer" });
  });
});
