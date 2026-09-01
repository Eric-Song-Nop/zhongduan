import { describe, expect, it } from "vitest";

import {
  CapabilityMintRequestSchema,
  CapabilityRefreshRequestSchema,
  CapabilityResponseSchema,
  ConnectionSetRequestSchema,
  ConnectionSetResponseSchema,
  HostCapabilityReclaimRequestSchema,
  CreateSessionRequestSchema,
  RelayCapability,
  selectRelayCapabilities,
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
        selectedCapabilities: [RelayCapability.deliveryBarrierOutcomeV1],
      }),
    ).toMatchObject({
      deliveryGeneration: "2",
      selectedCapabilities: [RelayCapability.deliveryBarrierOutcomeV1],
    });
    expect(
      ConnectionSetResponseSchema.parse({
        connectionSetId: "connection_set_AAAAA",
        connectionId: "connection_id_AAAAAA",
        clientId: null,
        streamId: 0,
        deliveryGeneration: "0",
        expiresAt: 1_800_000_000_000,
        controlTicket: "control_ticket_AAAAAA",
        dataTicket: "data_ticket_AAAAAAAAA",
      }),
    ).not.toHaveProperty("selectedCapabilities");
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

  it("selects the known intersection from a bounded capability header", () => {
    expect(
      selectRelayCapabilities(
        `future-relay-capability-v2,${RelayCapability.deliveryBarrierOutcomeV1}`,
      ),
    ).toEqual([RelayCapability.deliveryBarrierOutcomeV1]);
    expect(
      selectRelayCapabilities(
        `${RelayCapability.browserInputAdmissionV1},${RelayCapability.deliveryBarrierOutcomeV1},${RelayCapability.hostDataBatchV1},${RelayCapability.browserDataBatchV1}`,
      ),
    ).toEqual([
      RelayCapability.browserDataBatchV1,
      RelayCapability.browserInputAdmissionV1,
      RelayCapability.deliveryBarrierOutcomeV1,
      RelayCapability.hostDataBatchV1,
    ]);
    expect(selectRelayCapabilities("future-relay-capability-v2")).toEqual([]);
    expect(selectRelayCapabilities("INVALID")).toBeUndefined();
    expect(
      selectRelayCapabilities(Array.from({ length: 17 }, () => "future-capability").join(",")),
    ).toBeUndefined();
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
