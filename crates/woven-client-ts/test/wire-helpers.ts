import * as flatbuffers from "flatbuffers";
import {
  AuthenticatedPayload,
  CapabilitiesPayload,
  ControlPayload,
  DeliveryClass,
  Envelope as FbEnvelope,
  MessageKind,
  ProtocolErrorCode,
  ProtocolErrorPayload,
} from "../generated/woven/protocol/v1.js";

interface EnvelopeOptions {
  protocolVersion?: number;
  namespaceId?: bigint;
  sessionId?: bigint;
  spaceId?: bigint;
  entityId?: bigint;
  spaceEpoch?: bigint;
  correlationId?: bigint;
  channelId?: bigint;
}

export function createEnvelope(
  builder: flatbuffers.Builder,
  protocolVersion: number,
  kind: MessageKind,
  delivery: DeliveryClass,
  namespaceId: bigint,
  sessionId: bigint,
  spaceId: bigint,
  entityId: bigint,
  spaceEpoch: bigint,
  serverTick: bigint,
  senderSequence: bigint,
  correlationId: bigint,
  payloadTypeId: bigint,
  payload: number,
  controlType: ControlPayload,
  control: number,
  channelId: bigint,
  routingPosition = 0,
): number {
  FbEnvelope.startEnvelope(builder);
  FbEnvelope.addProtocolVersion(builder, protocolVersion);
  FbEnvelope.addMessageKind(builder, kind);
  FbEnvelope.addDeliveryClass(builder, delivery);
  FbEnvelope.addNamespaceId(builder, namespaceId);
  FbEnvelope.addSessionId(builder, sessionId);
  FbEnvelope.addSpaceId(builder, spaceId);
  FbEnvelope.addEntityId(builder, entityId);
  FbEnvelope.addSpaceEpoch(builder, spaceEpoch);
  FbEnvelope.addServerTick(builder, serverTick);
  FbEnvelope.addSenderSequence(builder, senderSequence);
  FbEnvelope.addCorrelationId(builder, correlationId);
  FbEnvelope.addPayloadTypeId(builder, payloadTypeId);
  FbEnvelope.addPayload(builder, payload);
  FbEnvelope.addControlType(builder, controlType);
  FbEnvelope.addControl(builder, control);
  FbEnvelope.addChannelId(builder, channelId);
  FbEnvelope.addRoutingPosition(builder, routingPosition);
  return FbEnvelope.endEnvelope(builder);
}

function finishControl(
  builder: flatbuffers.Builder,
  kind: MessageKind,
  controlType: ControlPayload,
  control: flatbuffers.Offset,
  options: EnvelopeOptions = {},
): Uint8Array {
  const root = createEnvelope(
    builder,
    options.protocolVersion ?? 1,
    kind,
    DeliveryClass.ReliableOrdered,
    options.namespaceId ?? 0n,
    options.sessionId ?? 0n,
    options.spaceId ?? 0n,
    options.entityId ?? 0n,
    options.spaceEpoch ?? 0n,
    0n,
    0n,
    options.correlationId ?? 0n,
    0n,
    0,
    controlType,
    control,
    options.channelId ?? 0n,
  );
  FbEnvelope.finishSizePrefixedEnvelopeBuffer(builder, root);
  return builder.asUint8Array();
}

export function buildCapabilities(options: {
  envelopeProtocolVersion?: number;
  selectedProtocolVersion?: number;
  maxFrameSize?: number;
  maxPayloadSize?: number;
  capabilityBits?: bigint;
  controlType?: ControlPayload;
} = {}): Uint8Array {
  const builder = new flatbuffers.Builder(256);
  const serverName = builder.createString("woven-test");
  const serverVersion = builder.createString("0.2.0");
  const payload = CapabilitiesPayload.createCapabilitiesPayload(
    builder,
    options.selectedProtocolVersion ?? 1,
    serverName,
    serverVersion,
    options.capabilityBits ?? 0n,
    options.maxFrameSize ?? 1_048_576,
    options.maxPayloadSize ?? 65_536,
  );
  return finishControl(
    builder,
    MessageKind.Capabilities,
    options.controlType ?? ControlPayload.CapabilitiesPayload,
    payload,
    { protocolVersion: options.envelopeProtocolVersion },
  );
}

export function buildAuthenticated(options: {
  principalId?: bigint;
  assignedEntityId?: bigint;
  controlType?: ControlPayload;
} = {}): Uint8Array {
  const builder = new flatbuffers.Builder(128);
  const payload = AuthenticatedPayload.createAuthenticatedPayload(
    builder,
    options.principalId ?? 1n,
    options.assignedEntityId ?? 0n,
  );
  return finishControl(
    builder,
    MessageKind.Authenticated,
    options.controlType ?? ControlPayload.AuthenticatedPayload,
    payload,
  );
}

export function buildProtocolError(options: {
  relatedKind: MessageKind;
  namespaceId?: bigint;
  sessionId?: bigint;
  correlationId?: bigint;
  code?: ProtocolErrorCode;
  message?: string;
}): Uint8Array {
  const builder = new flatbuffers.Builder(256);
  const message = builder.createString(options.message ?? "rejected");
  const payload = ProtocolErrorPayload.createProtocolErrorPayload(
    builder,
    options.code ?? ProtocolErrorCode.Unauthorized,
    options.relatedKind,
    message,
  );
  return finishControl(
    builder,
    MessageKind.ProtocolError,
    ControlPayload.ProtocolErrorPayload,
    payload,
    options,
  );
}
