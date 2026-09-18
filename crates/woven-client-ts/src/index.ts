export {
  WovenClient,
  type WovenConfig,
  type WovenError,
  type AdmissionResult,
  type QueueUpdate,
  type ManagedAdmissionOutcome,
} from "./client.js";
export { EnvelopeCodec, CodecError, PROTOCOL_VERSION, FILE_IDENTIFIER } from "./codec.js";
export type { DecodedEnvelope } from "./codec.js";
export {
  AuthenticationScheme,
  MessageKind,
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
  encodeSubscribeSpace,
  encodeSnapshotRequest,
  encodeSpaceTransition,
  encodeInferenceRequested,
  encodeReliableEvent,
  encodeEntityState,
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
