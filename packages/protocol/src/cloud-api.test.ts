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
  confirmedRelayCapabilities,
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

  it("decodes connection-set responses with or without the rolling capability echo", () => {
    expect(ConnectionSetRequestSchema.parse({ clientId: "client_id_AAAAAAAAAAA" })).toEqual({
      clientId: "client_id_AAAAAAAAAAA",
    });
    expect(() => ConnectionSetRequestSchema.parse({ clientId: "short" })).toThrow();
    expect(() =>
      ConnectionSetRequestSchema.parse({ clientId: "client_id_AAAAAAAAAAA", role: "host" }),
    ).toThrow();
    const selectedOnly = ConnectionSetResponseSchema.parse({
      connectionSetId: "connection_set_AAAAA",
      connectionId: "connection_id_AAAAAA",
      clientId: "client_id_AAAAAAAAAAA",
      streamId: 3,
      deliveryGeneration: "2",
      expiresAt: 1_800_000_000_000,
      controlTicket: "control_ticket_AAAAAA",
      dataTicket: "data_ticket_AAAAAAAAA",
      selectedCapabilities: [RelayCapability.deliveryBarrierOutcomeV1],
    });
    expect(selectedOnly).toMatchObject({
      deliveryGeneration: "2",
      selectedCapabilities: [RelayCapability.deliveryBarrierOutcomeV1],
    });
    expect(
      confirmedRelayCapabilities(selectedOnly, [
        RelayCapability.capabilityNegotiationV1,
        RelayCapability.deliveryBarrierOutcomeV1,
      ]),
    ).toEqual([]);
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

  it("decodes raw negotiated lists but confirms only an offered bootstrap handshake", () => {
    const common = {
      connectionSetId: "connection_set_AAAAA",
      connectionId: "connection_id_AAAAAA",
      clientId: null,
      streamId: 0,
      deliveryGeneration: "0",
      expiresAt: 1_800_000_000_000,
      controlTicket: "control_ticket_AAAAAA",
      dataTicket: "data_ticket_AAAAAAAAA",
    };
    const confirmed = ConnectionSetResponseSchema.parse({
      ...common,
      negotiatedCapabilities: [
        RelayCapability.capabilityNegotiationV1,
        RelayCapability.authorityDataV2,
      ],
    });
    expect(confirmed).toMatchObject({
      negotiatedCapabilities: [
        RelayCapability.capabilityNegotiationV1,
        RelayCapability.authorityDataV2,
      ],
    });
    expect(
      confirmedRelayCapabilities(confirmed, [
        RelayCapability.capabilityNegotiationV1,
        RelayCapability.authorityDataV2,
      ]),
    ).toEqual(confirmed.negotiatedCapabilities);

    const missingBootstrap = ConnectionSetResponseSchema.parse({
      ...common,
      negotiatedCapabilities: [RelayCapability.authorityDataV2],
    });
    expect(
      confirmedRelayCapabilities(missingBootstrap, [
        RelayCapability.capabilityNegotiationV1,
        RelayCapability.authorityDataV2,
      ]),
    ).toEqual([]);
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
