import * as flatbuffers from "flatbuffers";
import {
  Envelope as FbEnvelope,
  HelloPayload,
  AuthenticatePayload,
  JoinSessionPayload,
  LeaveSessionPayload,
  ClientLogPayload,
  LogLevel,
  SubscribeSpacePayload,
  SnapshotRequestPayload,
  SpaceTransitionPayload,
  InferenceRequestedPayload,
  RequestAdmissionPayload,
  QueueStatusRequestPayload,
  QueueHeartbeatPayload,
  QueueClaimPayload,
  QueueCancelPayload,
  AuthenticationScheme,
  ControlPayload,
  DeliveryClass,
  MessageKind,
  RoutingPosition3D as FbRoutingPosition3D,
} from "../generated/woven/protocol/v1.js";
import {
  DEFAULT_MAX_FRAME_BYTES,
  DEFAULT_MAX_PAYLOAD_BYTES,
  PROTOCOL_VERSION,
  CAPABILITY_POSITIONED_ENTITY_STATE,
  CAPABILITY_CLIENT_LOG,
  validateLogMessage,
  CodecError,
  RoutingPosition3D,
  validatePayloadLength,
} from "./codec.js";

/** Options shared by every outbound message. */
export interface EnvelopeScope {
  namespaceId?: bigint;
  sessionId?: bigint;
  spaceId?: bigint;
  spaceEpoch?: bigint;
  channelId?: bigint;
  entityId?: bigint;
  senderSequence?: bigint;
  correlationId?: bigint;
  routingPosition?: RoutingPosition3D;
}

/** Session scope required by every managed-admission request. */
export interface ManagedEnvelopeScope {
  namespaceId: bigint;
  sessionId: bigint;
  correlationId: bigint;
}

const zero = 0n;

/**
 * Encode a Hello control envelope as a size-prefixed frame. Mirrors the Rust
 * reference client's handshake start.
 */
export function encodeHello(opts: {
  clientName?: string;
  clientVersion?: string;
  maxFrameSize?: number;
  maxPayloadSize?: number;
  capabilityBits?: bigint;
}): Uint8Array {
  const clientName = opts.clientName ?? "woven-client-ts";
  const clientVersion = opts.clientVersion ?? "0.3.0";
  validatePayloadLength(
    new TextEncoder().encode(clientName).byteLength +
      new TextEncoder().encode(clientVersion).byteLength,
  );
  const builder = new flatbuffers.Builder(1024);
  const clientNameOffset = builder.createString(clientName);
  const clientVersionOffset = builder.createString(clientVersion);
  const control = HelloPayload.createHelloPayload(
    builder,
    1,
    1,
    clientNameOffset,
    clientVersionOffset,
    opts.capabilityBits ?? (CAPABILITY_POSITIONED_ENTITY_STATE | CAPABILITY_CLIENT_LOG),
    opts.maxFrameSize ?? DEFAULT_MAX_FRAME_BYTES,
    opts.maxPayloadSize ?? DEFAULT_MAX_PAYLOAD_BYTES,
  );
  return finishControl(builder, MessageKind.Hello, ControlPayload.HelloPayload, control, {
    deliveryClass: DeliveryClass.ReliableOrdered,
  });
}

/** Encode an Authenticate control envelope. Development remains the compatibility default. */
export function encodeAuthenticate(
  credentials: Uint8Array,
  scheme: AuthenticationScheme = AuthenticationScheme.Development,
): Uint8Array {
  if (scheme !== AuthenticationScheme.Development && scheme !== AuthenticationScheme.Bearer) {
    throw new Error("authentication scheme must be Development or Bearer");
  }
  validatePayloadLength(credentials.byteLength);
  const builder = new flatbuffers.Builder(256);
  const creds = builder.createByteVector(credentials);
  const control = AuthenticatePayload.createAuthenticatePayload(builder, scheme, creds);
  return finishControl(
    builder,
    MessageKind.Authenticate,
    ControlPayload.AuthenticatePayload,
    control,
    { deliveryClass: DeliveryClass.ReliableOrdered },
  );
}

/**
 * Encode a JoinSession control envelope.
 *
 * JoinSession is session-scoped, so `namespaceId` and `sessionId` must be nonzero.
 */
export function encodeJoinSession(
  scope: { namespaceId: bigint; sessionId: bigint },
  resumeToken?: Uint8Array,
): Uint8Array {
  validateNonzeroU64(scope.namespaceId, "namespace ID");
  validateNonzeroU64(scope.sessionId, "session ID");
  validatePayloadLength(resumeToken?.byteLength ?? 0);
  const builder = new flatbuffers.Builder(256);
  const token = builder.createByteVector(resumeToken ?? new Uint8Array(0));
  const control = JoinSessionPayload.createJoinSessionPayload(builder, token);
  return finishControl(
    builder,
    MessageKind.JoinSession,
    ControlPayload.JoinSessionPayload,
    control,
    { deliveryClass: DeliveryClass.ReliableOrdered, scope },
  );
}

/** Encode a session log; transport completion does not acknowledge server persistence. */
export function encodeClientLog(
  scope: EnvelopeScope & { namespaceId: bigint; sessionId: bigint },
  level: LogLevel,
  message: string,
): Uint8Array {
  validateSessionScope(scope);
  validatePayloadLength(validateLogMessage(level, message));
  const builder = new flatbuffers.Builder(1280);
  const text = builder.createString(message);
  const control = ClientLogPayload.createClientLogPayload(builder, level, text);
  return finishControl(builder, MessageKind.ClientLog, ControlPayload.ClientLogPayload, control, {
    deliveryClass: DeliveryClass.ReliableOrdered, scope,
  });
}

/** Encode an explicit session leave. */
export function encodeLeaveSession(
  scope: { namespaceId: bigint; sessionId: bigint },
  reason = "client request",
): Uint8Array {
  validateSessionScope(scope);
  validatePayloadLength(new TextEncoder().encode(reason).byteLength);
  const builder = new flatbuffers.Builder(256);
  const text = builder.createString(reason);
  const control = LeaveSessionPayload.createLeaveSessionPayload(builder, text);
  return finishControl(builder, MessageKind.LeaveSession, ControlPayload.LeaveSessionPayload, control, {
    deliveryClass: DeliveryClass.ReliableOrdered, scope,
  });
}

function validateSessionScope(scope: EnvelopeScope): void {
  validateNonzeroU64(scope.namespaceId ?? 0n, "namespace ID");
  validateNonzeroU64(scope.sessionId ?? 0n, "session ID");
  if ((scope.spaceId ?? 0n) !== 0n || (scope.spaceEpoch ?? 0n) !== 0n ||
      scope.channelId !== undefined || scope.entityId !== undefined ||
      scope.routingPosition !== undefined) {
    throw new CodecError("InvalidSemantics", "session control must not carry space, channel, entity, or epoch scope");
  }
}

/** Encode a managed admission request with a nonzero correlation ID. */
export function encodeRequestAdmission(
  scope: ManagedEnvelopeScope,
  idempotencyKey: string,
): Uint8Array {
  validateManagedScope(scope);
  const keyLength = new TextEncoder().encode(idempotencyKey).length;
  if (keyLength === 0 || keyLength > 256) {
    throw new Error("idempotency key must contain 1 to 256 UTF-8 bytes");
  }
  const builder = new flatbuffers.Builder(512);
  const key = builder.createString(idempotencyKey);
  const control = RequestAdmissionPayload.createRequestAdmissionPayload(builder, key);
  return finishControl(
    builder,
    MessageKind.RequestAdmission,
    ControlPayload.RequestAdmissionPayload,
    control,
    { deliveryClass: DeliveryClass.ReliableOrdered, scope },
  );
}

/** Encode a queue status request with a nonzero correlation and ticket ID. */
export function encodeQueueStatusRequest(
  scope: ManagedEnvelopeScope,
  ticketId: bigint,
): Uint8Array {
  return encodeQueueRequest(
    scope,
    ticketId,
    MessageKind.QueueStatusRequest,
    ControlPayload.QueueStatusRequestPayload,
    (builder) => QueueStatusRequestPayload.createQueueStatusRequestPayload(builder, ticketId),
  );
}

/** Encode a queue heartbeat with a nonzero correlation and ticket ID. */
export function encodeQueueHeartbeat(
  scope: ManagedEnvelopeScope,
  ticketId: bigint,
): Uint8Array {
  return encodeQueueRequest(
    scope,
    ticketId,
    MessageKind.QueueHeartbeat,
    ControlPayload.QueueHeartbeatPayload,
    (builder) => QueueHeartbeatPayload.createQueueHeartbeatPayload(builder, ticketId),
  );
}

/** Encode an offer claim with a nonzero correlation and ticket ID. */
export function encodeQueueClaim(
  scope: ManagedEnvelopeScope,
  ticketId: bigint,
): Uint8Array {
  return encodeQueueRequest(
    scope,
    ticketId,
    MessageKind.QueueClaim,
    ControlPayload.QueueClaimPayload,
    (builder) => QueueClaimPayload.createQueueClaimPayload(builder, ticketId),
  );
}

/** Encode a queue cancellation with a nonzero correlation and ticket ID. */
export function encodeQueueCancel(
  scope: ManagedEnvelopeScope,
  ticketId: bigint,
): Uint8Array {
  return encodeQueueRequest(
    scope,
    ticketId,
    MessageKind.QueueCancel,
    ControlPayload.QueueCancelPayload,
    (builder) => QueueCancelPayload.createQueueCancelPayload(builder, ticketId),
  );
}

/** Encode a SubscribeSpace control envelope. */
export function encodeSubscribeSpace(
  scope: EnvelopeScope & { channelId: bigint },
): Uint8Array {
  const builder = new flatbuffers.Builder(256);
  const control = SubscribeSpacePayload.createSubscribeSpacePayload(
    builder,
    scope.channelId,
  );
  return finishControl(
    builder,
    MessageKind.SubscribeSpace,
    ControlPayload.SubscribeSpacePayload,
    control,
    { deliveryClass: DeliveryClass.ReliableOrdered, scope },
  );
}

/** Encode a SnapshotRequest control envelope. */
export function encodeSnapshotRequest(
  scope: EnvelopeScope & { channelId: bigint },
  afterServerTick?: bigint,
): Uint8Array {
  const builder = new flatbuffers.Builder(256);
  const control = SnapshotRequestPayload.createSnapshotRequestPayload(
    builder,
    scope.channelId,
    afterServerTick ?? zero,
  );
  return finishControl(
    builder,
    MessageKind.SnapshotRequest,
    ControlPayload.SnapshotRequestPayload,
    control,
    { deliveryClass: DeliveryClass.ReliableOrdered, scope },
  );
}

/** Encode a SpaceTransition control envelope. */
export function encodeSpaceTransition(
  scope: EnvelopeScope & { entityId: bigint },
  opts: { fromSpaceId: bigint; toSpaceId: bigint; toSpaceEpoch: bigint },
): Uint8Array {
  const builder = new flatbuffers.Builder(256);
  const control = SpaceTransitionPayload.createSpaceTransitionPayload(
    builder,
    opts.fromSpaceId,
    opts.toSpaceId,
    opts.toSpaceEpoch,
  );
  return finishControl(
    builder,
    MessageKind.SpaceTransition,
    ControlPayload.SpaceTransitionPayload,
    control,
    { deliveryClass: DeliveryClass.ReliableOrdered, scope },
  );
}

/** Encode an InferenceRequested control envelope. */
export function encodeInferenceRequested(
  scope: EnvelopeScope & { entityId: bigint },
  opts: { capability: string; deadlineMs: bigint; input: Uint8Array },
): Uint8Array {
  validatePayloadLength(
    new TextEncoder().encode(opts.capability).byteLength + opts.input.byteLength,
  );
  const builder = new flatbuffers.Builder(1024);
  const capability = builder.createString(opts.capability);
  const input = builder.createByteVector(opts.input);
  const control = InferenceRequestedPayload.createInferenceRequestedPayload(
    builder,
    capability,
    opts.deadlineMs,
    input,
  );
  return finishControl(
    builder,
    MessageKind.InferenceRequested,
    ControlPayload.InferenceRequestedPayload,
    control,
    { deliveryClass: DeliveryClass.ReliableOrdered, scope },
  );
}

/** Encode a reliable opaque event payload envelope. */
export function encodeReliableEvent(
  scope: EnvelopeScope & { channelId: bigint; entityId: bigint },
  payload: { typeId: bigint; bytes: Uint8Array },
): Uint8Array {
  return encodeOpaque(
    MessageKind.ReliableEvent,
    DeliveryClass.ReliableOrdered,
    scope,
    payload,
  );
}

/** Encode a latest-value (entity state) opaque payload envelope. */
export function encodeEntityState(
  scope: EnvelopeScope & { channelId: bigint; entityId: bigint },
  payload: { typeId: bigint; bytes: Uint8Array },
): Uint8Array {
  return encodeOpaque(
    MessageKind.EntityState,
    DeliveryClass.LatestValue,
    scope,
    payload,
  );
}

/** Encode opaque entity state with fixed UnreliableSequenced delivery, never a reliable fallback. */
export function encodeUnreliableEntityState(
  scope: EnvelopeScope & { channelId: bigint; entityId: bigint },
  payload: { typeId: bigint; bytes: Uint8Array },
): Uint8Array {
  validatePayloadLength(payload.bytes.byteLength);
  for (const [name, value] of [
    ["namespace ID", scope.namespaceId],
    ["session ID", scope.sessionId],
    ["space ID", scope.spaceId],
    ["space epoch", scope.spaceEpoch],
    ["channel ID", scope.channelId],
    ["entity ID", scope.entityId],
    ["payload type ID", payload.typeId],
  ] as const) {
    if (typeof value !== "bigint" || value <= 0n || value > 0xffff_ffff_ffff_ffffn) {
      throw new CodecError("InvalidSemantics", `${name} must be a nonzero u64`);
    }
  }
  // WVN1 reserves zero IDs, but permits zero as an initial sender sequence.
  const sequence = scope.senderSequence ?? zero;
  if (typeof sequence !== "bigint" || sequence < 0n || sequence > 0xffff_ffff_ffff_ffffn) {
    throw new CodecError("InvalidSemantics", "sender sequence must be a u64");
  }
  return encodeOpaque(MessageKind.EntityState, DeliveryClass.UnreliableSequenced, scope, payload);
}

function encodeOpaque(
  kind: MessageKind,
  deliveryClass: DeliveryClass,
  scope: EnvelopeScope & { channelId: bigint; entityId: bigint },
  payload: { typeId: bigint; bytes: Uint8Array },
): Uint8Array {
  validatePayloadLength(payload.bytes.byteLength);
  if (scope.routingPosition !== undefined && kind !== MessageKind.EntityState) {
    throw new CodecError("InvalidSemantics", "routing position is valid only on EntityState");
  }
  const builder = new flatbuffers.Builder(1024);
  const payloadOffset = builder.createByteVector(payload.bytes);
  const routingPositionOffset = createRoutingPosition(builder, scope.routingPosition);
  const root = createEnvelope(
    builder,
    PROTOCOL_VERSION,
    kind,
    deliveryClass,
    scope.namespaceId ?? zero,
    scope.sessionId ?? zero,
    scope.spaceId ?? zero,
    scope.entityId ?? zero,
    scope.spaceEpoch ?? zero,
    zero,
    scope.senderSequence ?? zero,
    zero,
    payload.typeId,
    payloadOffset,
    ControlPayload.NONE,
    0,
    scope.channelId ?? zero,
    routingPositionOffset,
  );
  finishEnvelope(builder, root);
  return builder.asUint8Array();
}

function encodeQueueRequest(
  scope: ManagedEnvelopeScope,
  ticketId: bigint,
  kind: MessageKind,
  controlType: ControlPayload,
  createControl: (builder: flatbuffers.Builder) => number,
): Uint8Array {
  validateManagedScope(scope);
  validateNonzeroU64(ticketId, "ticket ID");
  const builder = new flatbuffers.Builder(256);
  return finishControl(builder, kind, controlType, createControl(builder), {
    deliveryClass: DeliveryClass.ReliableOrdered,
    scope,
  });
}

function validateManagedScope(scope: ManagedEnvelopeScope): void {
  validateNonzeroU64(scope.namespaceId, "namespace ID");
  validateNonzeroU64(scope.sessionId, "session ID");
  validateNonzeroU64(scope.correlationId, "correlation ID");
}

function validateNonzeroU64(value: bigint, name: string): void {
  if (value <= 0n || value > 0xffff_ffff_ffff_ffffn) {
    throw new Error(`${name} must be a nonzero u64`);
  }
}

function finishControl(
  builder: flatbuffers.Builder,
  kind: MessageKind,
  controlType: ControlPayload,
  controlOffset: number,
  opts: { deliveryClass: DeliveryClass; scope?: EnvelopeScope },
): Uint8Array {
  const scope = opts.scope ?? {};
  if (scope.routingPosition !== undefined) {
    throw new CodecError("InvalidSemantics", "routing position is valid only on EntityState");
  }
  const root = createEnvelope(
    builder,
    PROTOCOL_VERSION,
    kind,
    opts.deliveryClass,
    scope.namespaceId ?? zero,
    scope.sessionId ?? zero,
    scope.spaceId ?? zero,
    scope.entityId ?? zero,
    scope.spaceEpoch ?? zero,
    zero,
    scope.senderSequence ?? zero,
    scope.correlationId ?? zero,
    zero,
    0,
    controlType,
    controlOffset,
    scope.channelId ?? zero,
    0,
  );
  finishEnvelope(builder, root);
  return builder.asUint8Array();
}

function createRoutingPosition(
  builder: flatbuffers.Builder,
  position: RoutingPosition3D | undefined,
): number {
  if (position === undefined) return 0;
  if (![position.x, position.y, position.z].every(Number.isFinite)) {
    throw new CodecError("InvalidSemantics", "routing position coordinates must be finite");
  }
  return FbRoutingPosition3D.createRoutingPosition3D(
    builder,
    position.x,
    position.y,
    position.z,
  );
}

function createEnvelope(
  builder: flatbuffers.Builder,
  protocolVersion: number,
  kind: MessageKind,
  deliveryClass: DeliveryClass,
  namespaceId: bigint,
  sessionId: bigint,
  spaceId: bigint,
  entityId: bigint,
  spaceEpoch: bigint,
  serverTick: bigint,
  senderSequence: bigint,
  correlationId: bigint,
  payloadTypeId: bigint,
  payloadOffset: number,
  controlType: ControlPayload,
  controlOffset: number,
  channelId: bigint,
  routingPositionOffset: number,
): number {
  FbEnvelope.startEnvelope(builder);
  FbEnvelope.addProtocolVersion(builder, protocolVersion);
  FbEnvelope.addMessageKind(builder, kind);
  FbEnvelope.addDeliveryClass(builder, deliveryClass);
  FbEnvelope.addNamespaceId(builder, namespaceId);
  FbEnvelope.addSessionId(builder, sessionId);
  FbEnvelope.addSpaceId(builder, spaceId);
  FbEnvelope.addEntityId(builder, entityId);
  FbEnvelope.addSpaceEpoch(builder, spaceEpoch);
  FbEnvelope.addServerTick(builder, serverTick);
  FbEnvelope.addSenderSequence(builder, senderSequence);
  FbEnvelope.addCorrelationId(builder, correlationId);
  FbEnvelope.addPayloadTypeId(builder, payloadTypeId);
  FbEnvelope.addPayload(builder, payloadOffset);
  FbEnvelope.addControlType(builder, controlType);
  FbEnvelope.addControl(builder, controlOffset);
  FbEnvelope.addChannelId(builder, channelId);
  FbEnvelope.addRoutingPosition(builder, routingPositionOffset);
  return FbEnvelope.endEnvelope(builder);
}

function finishEnvelope(
  builder: flatbuffers.Builder,
  root: number,
): void {
  FbEnvelope.finishSizePrefixedEnvelopeBuffer(builder, root);
}
