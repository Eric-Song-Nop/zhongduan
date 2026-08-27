import { describe, expect, it } from "vitest";

import {
  CapabilityMintRequestSchema,
  CapabilityRefreshRequestSchema,
  CapabilityResponseSchema,
  ConnectionSetRequestSchema,
  ConnectionSetResponseSchema,
  HostCapabilityReclaimRequestSchema,
} from "./cloud-api";

describe("Cloud HTTP contracts", () => {
  it("validates a generation-fenced connection-set response", () => {
    expect(ConnectionSetRequestSchema.parse({ clientId: "client_id_AAAAAAAAAAA" })).toEqual({
      clientId: "client_id_AAAAAAAAAAA",
    });
    expect(() => ConnectionSetRequestSchema.parse({ clientId: "short" })).toThrow();
    expect(() =>
      ConnectionSetRequestSchema.parse({ clientId: "client_id_AAAAAAAAAAA", role: "host" }),
    ).toThrow();
    expect(
      ConnectionSetResponseSchema.parse({
        connectionSetId: "connection_set_AAAAA",
        connectionId: "connection_id_AAAAAA",
        clientId: "client_id_AAAAAAAAAAA",
        streamId: 3,
        deliveryGeneration: "2",
        expiresAt: 1_800_000_000_000,
        controlTicket: "control_ticket_AAAAAA",
        dataTicket: "data_ticket_AAAAAAAAA",
      }),
    ).toMatchObject({ deliveryGeneration: "2" });
  });

  it("keeps capability roles and reclaim identity strict", () => {
    expect(CapabilityMintRequestSchema.parse({ role: "writer" })).toEqual({ role: "writer" });
    expect(CapabilityRefreshRequestSchema.parse({})).toEqual({});
    expect(() => CapabilityMintRequestSchema.parse({ role: "host" })).toThrow();
    expect(() => CapabilityRefreshRequestSchema.parse({ rotateRole: "host" })).toThrow();
    expect(
      HostCapabilityReclaimRequestSchema.parse({ engineId: "engine", sessionEpoch: "7" }),
    ).toEqual({ engineId: "engine", sessionEpoch: "7" });
    expect(() =>
      HostCapabilityReclaimRequestSchema.parse({ engineId: "engine", sessionEpoch: "0" }),
    ).toThrow();
  });

  it("bounds issued capabilities", () => {
    expect(
      CapabilityResponseSchema.parse({
        capability: "zcap1.payload.signature",
        expiresAt: 1_800_000_000,
        role: "observer",
      }),
    ).toMatchObject({ role: "observer" });
  });
});
