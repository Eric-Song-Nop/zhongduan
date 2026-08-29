import { describe, expect, it } from "vitest";

import {
  RelayCapability,
  confirmedRelayCapabilities,
  selectRecoveryStrategy,
  selectRelayCapabilities,
} from "./wire-capabilities";

const hostV3 = [
  RelayCapability.capabilityNegotiationV1,
  RelayCapability.authorityDataV2,
  RelayCapability.wireEndpointV3,
  RelayCapability.recoveryV3GapFillV1,
] as const;
const browserV3 = [
  RelayCapability.capabilityNegotiationV1,
  RelayCapability.authorityDataV2,
  RelayCapability.wireEndpointV3,
  RelayCapability.deliveryEnvelopeV3,
  RelayCapability.deliveryReceiveCreditV1,
  RelayCapability.authorityApplyProgressV1,
  RelayCapability.recoveryV3GapFillV1,
] as const;

describe("wire capability negotiation", () => {
  it("requires the bootstrap token before trusting a negotiated response", () => {
    expect(confirmedRelayCapabilities({}, hostV3)).toEqual([]);
    expect(
      confirmedRelayCapabilities(
        { negotiatedCapabilities: [RelayCapability.authorityDataV2] },
        hostV3,
      ),
    ).toEqual([]);
    expect(confirmedRelayCapabilities({ negotiatedCapabilities: hostV3 }, hostV3)).toEqual(hostV3);
    expect(confirmedRelayCapabilities({ negotiatedCapabilities: browserV3 }, hostV3)).toEqual([]);
  });

  it("selects known tokens in canonical server order and ignores future tokens", () => {
    expect(
      selectRelayCapabilities(
        `${RelayCapability.authorityDataV2},future-capability,${RelayCapability.capabilityNegotiationV1}`,
      ),
    ).toEqual([RelayCapability.capabilityNegotiationV1, RelayCapability.authorityDataV2]);
  });

  it("cannot select recovery v3 unless the explicit production gate and every owner agree", () => {
    const common = {
      authorityDataVersion: 2 as const,
      hostCapabilities: hostV3,
      browserCapabilities: browserV3,
      cloudCapabilities: browserV3,
    };

    expect(selectRecoveryStrategy({ ...common, enabled: false })).toBe("v2");
    expect(selectRecoveryStrategy({ ...common, enabled: true })).toBe("v3");
    const missingOwnerCapability = [
      {
        ...common,
        hostCapabilities: hostV3.filter(
          (capability) => capability !== RelayCapability.recoveryV3GapFillV1,
        ),
      },
      {
        ...common,
        cloudCapabilities: browserV3.filter(
          (capability) => capability !== RelayCapability.deliveryEnvelopeV3,
        ),
      },
      {
        ...common,
        enabled: true,
        browserCapabilities: browserV3.filter(
          (capability) => capability !== RelayCapability.authorityApplyProgressV1,
        ),
      },
    ];
    for (const capabilities of missingOwnerCapability) {
      expect(selectRecoveryStrategy({ ...capabilities, enabled: true })).toBe("v2");
    }
  });
});
