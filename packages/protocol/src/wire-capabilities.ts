import { z } from "zod";

export const RELAY_CAPABILITIES_HEADER = "x-zhongduan-relay-capabilities";

export const RelayCapability = {
  capabilityNegotiationV1: "capability-negotiation-v1",
  authorityDataV2: "authority-data-v2",
  deliveryBarrierOutcomeV1: "delivery-barrier-outcome-v1",
  wireEndpointV3: "wire-endpoint-v3",
  deliveryEnvelopeV3: "delivery-envelope-v3",
  deliveryReceiveCreditV1: "delivery-receive-credit-v1",
  authorityApplyProgressV1: "authority-apply-progress-v1",
  recoveryV3GapFillV1: "recovery-v3-gap-fill-v1",
} as const;

export const RelayCapabilitySchema = z.enum([
  RelayCapability.capabilityNegotiationV1,
  RelayCapability.authorityDataV2,
  RelayCapability.deliveryBarrierOutcomeV1,
  RelayCapability.wireEndpointV3,
  RelayCapability.deliveryEnvelopeV3,
  RelayCapability.deliveryReceiveCreditV1,
  RelayCapability.authorityApplyProgressV1,
  RelayCapability.recoveryV3GapFillV1,
]);
export type RelayCapability = z.infer<typeof RelayCapabilitySchema>;

const MAX_RELAY_CAPABILITIES_HEADER_CHARS = 1_024;
const MAX_RELAY_CAPABILITIES = 16;
const relayCapabilityToken = /^[a-z0-9][a-z0-9-]{0,63}$/u;
const canonicalCapabilities = Object.values(RelayCapability);

export function selectRelayCapabilities(header: string | null): RelayCapability[] | undefined {
  if (header === null || header === "") return [];
  if (header.length > MAX_RELAY_CAPABILITIES_HEADER_CHARS) return undefined;
  const requested = header.split(",").map((entry) => entry.trim());
  if (
    requested.length > MAX_RELAY_CAPABILITIES ||
    requested.some((entry) => !relayCapabilityToken.test(entry))
  ) {
    return undefined;
  }
  const requestedSet = new Set(requested);
  return canonicalCapabilities.filter((capability) => requestedSet.has(capability));
}

export interface NegotiatedRelayCapabilitiesView {
  readonly negotiatedCapabilities?: readonly RelayCapability[] | undefined;
}

export function confirmedRelayCapabilities(
  response: NegotiatedRelayCapabilitiesView,
  offered: readonly RelayCapability[],
): RelayCapability[] {
  const negotiated = response.negotiatedCapabilities;
  if (
    negotiated === undefined ||
    !offered.includes(RelayCapability.capabilityNegotiationV1) ||
    !negotiated.includes(RelayCapability.capabilityNegotiationV1)
  ) {
    return [];
  }
  const offeredSet = new Set(offered);
  const negotiatedSet = new Set(negotiated);
  if (
    offeredSet.size !== offered.length ||
    negotiatedSet.size !== negotiated.length ||
    negotiated.some((capability) => !offeredSet.has(capability))
  ) {
    return [];
  }
  return canonicalCapabilities.filter((capability) => negotiatedSet.has(capability));
}

export const RecoveryStrategySchema = z.enum(["v2", "v3"]);
export type RecoveryStrategy = z.infer<typeof RecoveryStrategySchema>;

export interface RecoveryStrategySelectorInput {
  readonly authorityDataVersion: 2;
  readonly browserCapabilities: readonly RelayCapability[];
  readonly cloudCapabilities: readonly RelayCapability[];
  readonly enabled: boolean;
  readonly hostCapabilities: readonly RelayCapability[];
}

const commonRecoveryV3Capabilities = [
  RelayCapability.capabilityNegotiationV1,
  RelayCapability.authorityDataV2,
  RelayCapability.wireEndpointV3,
  RelayCapability.recoveryV3GapFillV1,
] as const;
const browserRecoveryV3Capabilities = [
  ...commonRecoveryV3Capabilities,
  RelayCapability.deliveryEnvelopeV3,
  RelayCapability.deliveryReceiveCreditV1,
  RelayCapability.authorityApplyProgressV1,
] as const;

export function selectRecoveryStrategy(input: RecoveryStrategySelectorInput): RecoveryStrategy {
  if (!input.enabled || input.authorityDataVersion !== 2) return "v2";
  const host = new Set(input.hostCapabilities);
  const browser = new Set(input.browserCapabilities);
  const cloud = new Set(input.cloudCapabilities);
  const hostReady = commonRecoveryV3Capabilities.every(
    (capability) => host.has(capability) && cloud.has(capability),
  );
  const browserReady = browserRecoveryV3Capabilities.every(
    (capability) => browser.has(capability) && cloud.has(capability),
  );
  return hostReady && browserReady ? "v3" : "v2";
}
