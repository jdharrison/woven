export {
  WovenClient,
  type WovenConfig,
  type ClientLogger,
  type WovenError,
  type AdmissionResult,
  type QueueUpdate,
  type ManagedAdmissionOutcome,
} from "./client.js";
export {
  EnvelopeCodec,
  CodecError,
  PROTOCOL_VERSION,
  FILE_IDENTIFIER,
  CAPABILITY_POSITIONED_ENTITY_STATE,
  CAPABILITY_CLIENT_LOG,
  MAX_LOG_MESSAGE_BYTES,
} from "./codec.js";
export type { DecodedEnvelope, RoutingPosition3D } from "./codec.js";
export {
  AuthenticationScheme,
  DeliveryClass,
  MessageKind,
  LogLevel,
  ClientLogPayload,
  RequestAdmissionPayload,
  AdmissionResultPayload,
  AdmissionStatus,
  AdmissionRejectionCode,
  QueueStatusRequestPayload,
  QueueHeartbeatPayload,
  QueueClaimPayload,
  QueueCancelPayload,
  QueueUpdatePayload,
  QueueState,
} from "../generated/woven/protocol/v1.js";
export {
  encodeHello,
  encodeAuthenticate,
  encodeJoinSession,
  encodeLeaveSession,
  encodeClientLog,
  encodeSubscribeSpace,
  encodeSnapshotRequest,
  encodeSpaceTransition,
  encodeInferenceRequested,
  encodeReliableEvent,
  encodeEntityState,
  encodeUnreliableEntityState,
  encodeRequestAdmission,
  encodeQueueStatusRequest,
  encodeQueueHeartbeat,
  encodeQueueClaim,
  encodeQueueCancel,
} from "./encode.js";
export type { EnvelopeScope, ManagedEnvelopeScope } from "./encode.js";
export {
  TRANSFORM_ENCODED_LENGTH,
  encodeTransform,
  decodeTransform,
  type Transform,
} from "./transform.js";
export {
  resolveWebTransportConstructor,
  type WebTransport,
  type WebTransportOptions,
  type WebTransportHash,
  type WebTransportBidirectionalStream,
  type WebTransportDatagramDuplexStream,
  type WebTransportCloseInfo,
} from "./webtransport.js";
