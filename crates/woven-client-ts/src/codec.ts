import * as flatbuffers from "flatbuffers";
import {
  Envelope as FbEnvelope,
  DeliveryClass,
  MessageKind,
  ControlPayload,
  CapabilitiesPayload,
  AuthenticatedPayload,
  HelloPayload,
  AuthenticatePayload,
  JoinSessionPayload,
  LeaveSessionPayload,
  SubscriptionRejectedPayload,
  ProtocolErrorPayload,
  ProtocolErrorCode,
  InferenceRequestedPayload,
  InferenceStreamChunkPayload,
  InferenceCompletedPayload,
  InferenceFailedPayload,
  InferenceCancelledPayload,
  InferenceExpiredPayload,
  ToolCallProposedPayload,
  ToolCallAcceptedPayload,
  ToolCallRejectedPayload,
  ToolCallCompletedPayload,
  RequestAdmissionPayload, AdmissionResultPayload, AdmissionStatus, AdmissionRejectionCode,
  QueueStatusRequestPayload, QueueHeartbeatPayload, QueueClaimPayload, QueueCancelPayload,
  QueueUpdatePayload, QueueState,
} from "../generated/woven/protocol/v1.js";
import { unionToControlPayload } from "../generated/woven/protocol/v1/control-payload.js";

export const PROTOCOL_VERSION = 1;
export const FILE_IDENTIFIER = "WVN1";
export const DEFAULT_MAX_FRAME_BYTES = 1024 * 1024;
export const DEFAULT_MAX_PAYLOAD_BYTES = 256 * 1024;

/** Minimum frame length: 4-byte size prefix + root table offset. */
const MIN_FRAME_LENGTH = 12;
const MAX_SIZE_PREFIX = 0xffff_ffff;

/** A framing or decoding error, mirroring the Rust `CodecError` variants. */
export class CodecError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.name = "CodecError";
    this.code = code;
  }
}

function readUint32LE(bytes: Uint8Array, offset: number): number {
  return (
    bytes[offset]! +
    bytes[offset + 1]! * 0x100 +
    bytes[offset + 2]! * 0x10000 +
    bytes[offset + 3]! * 0x1000000
  );
}

function nonzero(value: bigint): bigint | null {
  return value === 0n ? null : value;
}

/** Normalized, language-neutral view of a decoded Woven envelope. */
export interface DecodedEnvelope {
  protocolVersion: number;
  messageKind: MessageKind;
  deliveryClass: DeliveryClass;
  namespaceId: bigint;
  sessionId: bigint;
  spaceId: bigint;
  entityId: bigint | null;
  spaceEpoch: bigint;
  serverTick: bigint;
  senderSequence: bigint;
  correlationId: bigint | null;
  channelId: bigint | null;
  payloadTypeId: bigint;
  payload: Uint8Array | null;
  controlType: ControlPayload;
  /** Typed control payload from the generated bindings, or `null` for opaque messages. */
  control: unknown;
}

/**
 * Size-prefixed FlatBuffers framing for Woven envelopes.
 *
 * A frame is a 4-byte little-endian size prefix followed by the FlatBuffers
 * buffer (which carries the `WVN1` file identifier). The size prefix stores the
 * length of the data that follows it, so a frame of N bytes has prefix value
 * N - 4. This is byte-for-byte compatible with the Rust `Codec`.
 */
export class EnvelopeCodec {
  readonly maxFrameBytes: number;
  readonly maxPayloadBytes: number;

  constructor(
    maxFrameBytes = DEFAULT_MAX_FRAME_BYTES,
    maxPayloadBytes = DEFAULT_MAX_PAYLOAD_BYTES,
  ) {
    validateLimits(maxFrameBytes, maxPayloadBytes);
    this.maxFrameBytes = maxFrameBytes;
    this.maxPayloadBytes = maxPayloadBytes;
  }

  /** Return the complete frame length as soon as a four-byte prefix is available. */
  expectedFrameLength(prefixBytes: Uint8Array): number | null {
    if (prefixBytes.length < 4) return null;
    const prefix = readUint32LE(prefixBytes, 0);
    if (prefix < 8) {
      throw new CodecError("InvalidSizePrefix", `invalid size prefix ${prefix}`);
    }
    const frameLength = prefix + 4;
    if (frameLength > this.maxFrameBytes) {
      throw new CodecError(
        "FrameTooLarge",
        `frame length ${frameLength} exceeds limit ${this.maxFrameBytes}`,
      );
    }
    return frameLength;
  }

  /** Decode a full size-prefixed frame into a {@link DecodedEnvelope}. */
  decode(frame: Uint8Array): DecodedEnvelope {
    const expectedLength = this.expectedFrameLength(frame);
    if (expectedLength === null) {
      throw new CodecError(
        "TruncatedFrame",
        `frame too short: ${frame.length} bytes`,
      );
    }
    if (expectedLength !== frame.length) {
      throw new CodecError(
        frame.length < expectedLength ? "TruncatedFrame" : "TrailingBytes",
        `expected ${expectedLength} bytes frame, got ${frame.length}`,
      );
    }
    if (frame.length < MIN_FRAME_LENGTH) {
      throw new CodecError(
        "TruncatedFrame",
        `frame too short: ${frame.length} bytes`,
      );
    }
    if (
      String.fromCharCode(
        frame[8] ?? 0,
        frame[9] ?? 0,
        frame[10] ?? 0,
        frame[11] ?? 0,
      ) !== FILE_IDENTIFIER
    ) {
      throw new CodecError(
        "InvalidFileIdentifier",
        `missing ${FILE_IDENTIFIER} file identifier`,
      );
    }
    try {
      const bb = new flatbuffers.ByteBuffer(
        new Uint8Array(frame.buffer, frame.byteOffset, frame.length),
      );
      const envelope = decodeEnvelope(FbEnvelope.getSizePrefixedRootAsEnvelope(bb));
      const payloadLength =
        (envelope.payload?.byteLength ?? 0) + controlVariableLength(envelope.control);
      if (payloadLength > this.maxPayloadBytes) {
        throw new CodecError(
          "PayloadTooLarge",
          `payload length ${payloadLength} exceeds limit ${this.maxPayloadBytes}`,
        );
      }
      return envelope;
    } catch (error) {
      if (error instanceof CodecError) throw error;
      throw new CodecError(
        "InvalidFlatbuffer",
        error instanceof Error ? error.message : "invalid FlatBuffer envelope",
      );
    }
  }

  /**
   * Handle one frame from a byte stream given the accumulated buffer. Returns
   * the decoded envelope and how many bytes were consumed, or `null` when more
   * bytes are required to complete a frame.
   */
  decodeStream(
    acc: Uint8Array,
  ): { envelope: DecodedEnvelope; consumed: number } | null {
    const frameLength = this.expectedFrameLength(acc);
    if (frameLength === null || acc.length < frameLength) return null;
    const frame = acc.subarray(0, frameLength);
    return { envelope: this.decode(frame), consumed: frameLength };
  }
}

function encodedStringLength(value: string | null): number {
  return value === null ? 0 : new TextEncoder().encode(value).byteLength;
}

function vectorLength(value: Uint8Array | null): number {
  return value?.byteLength ?? 0;
}

function controlVariableLength(control: unknown): number {
  if (control instanceof HelloPayload) {
    return encodedStringLength(control.clientName()) + encodedStringLength(control.clientVersion());
  }
  if (control instanceof CapabilitiesPayload) {
    return encodedStringLength(control.serverName()) + encodedStringLength(control.serverVersion());
  }
  if (control instanceof RequestAdmissionPayload) {
    return encodedStringLength(control.idempotencyKey());
  }
  if (control instanceof AuthenticatePayload) return vectorLength(control.credentialsArray());
  if (control instanceof JoinSessionPayload) return vectorLength(control.resumeTokenArray());
  if (control instanceof LeaveSessionPayload) return encodedStringLength(control.reason());
  if (control instanceof SubscriptionRejectedPayload) {
    return encodedStringLength(control.reason());
  }
  if (control instanceof ProtocolErrorPayload) return encodedStringLength(control.message());
  if (control instanceof InferenceRequestedPayload) {
    return encodedStringLength(control.capability()) + vectorLength(control.inputArray());
  }
  if (control instanceof InferenceStreamChunkPayload) return vectorLength(control.chunkArray());
  if (control instanceof InferenceCompletedPayload) return vectorLength(control.resultArray());
  if (control instanceof InferenceFailedPayload) return encodedStringLength(control.reason());
  if (control instanceof InferenceCancelledPayload) return encodedStringLength(control.reason());
  if (control instanceof InferenceExpiredPayload) return encodedStringLength(control.reason());
  if (control instanceof ToolCallProposedPayload) {
    return encodedStringLength(control.toolId()) + vectorLength(control.argumentsArray());
  }
  if (control instanceof ToolCallAcceptedPayload) return encodedStringLength(control.toolId());
  if (control instanceof ToolCallRejectedPayload) return encodedStringLength(control.reason());
  if (control instanceof ToolCallCompletedPayload) return vectorLength(control.resultArray());
  return 0;
}

function validateLimits(maxFrameBytes: number, maxPayloadBytes: number): void {
  const valid =
    Number.isInteger(maxFrameBytes) &&
    Number.isInteger(maxPayloadBytes) &&
    maxFrameBytes >= MIN_FRAME_LENGTH &&
    maxFrameBytes <= MAX_SIZE_PREFIX + 4 &&
    maxPayloadBytes > 0 &&
    maxPayloadBytes <= maxFrameBytes;
  if (!valid) {
    throw new CodecError(
      "InvalidLimits",
      `invalid codec limits: frame=${maxFrameBytes}, payload=${maxPayloadBytes}`,
    );
  }
}

function decodeEnvelope(envelope: FbEnvelope): DecodedEnvelope {
  const controlType = envelope.controlType();
  const decoded: DecodedEnvelope = {
    protocolVersion: envelope.protocolVersion(),
    messageKind: envelope.messageKind(),
    deliveryClass: envelope.deliveryClass(),
    namespaceId: envelope.namespaceId(),
    sessionId: envelope.sessionId(),
    spaceId: envelope.spaceId(),
    entityId: nonzero(envelope.entityId()),
    spaceEpoch: envelope.spaceEpoch(),
    serverTick: envelope.serverTick(),
    senderSequence: envelope.senderSequence(),
    correlationId: nonzero(envelope.correlationId()),
    channelId: nonzero(envelope.channelId()),
    payloadTypeId: envelope.payloadTypeId(),
    payload: envelope.payloadArray(),
    controlType,
    control:
      controlType === ControlPayload.NONE
        ? null
        : unionToControlPayload(controlType, (obj) => envelope.control(obj)),
  };
  validateCommon(decoded);
  validateHandshake(decoded);
  validateProtocolError(decoded);
  validateManaged(decoded);
  return decoded;
}

function requireSemantics(valid: boolean, message: string): void {
  if (!valid) throw new CodecError("InvalidSemantics", message);
}

function hasNoDomainPayload(e: DecodedEnvelope): boolean {
  return e.payloadTypeId === 0n && (e.payload === null || e.payload.length === 0);
}

function isConnectionScoped(e: DecodedEnvelope): boolean {
  return (
    e.namespaceId === 0n &&
    e.sessionId === 0n &&
    e.spaceId === 0n &&
    e.spaceEpoch === 0n &&
    e.channelId === null &&
    e.entityId === null
  );
}

function validateCommon(e: DecodedEnvelope): void {
  requireSemantics(
    e.protocolVersion === PROTOCOL_VERSION,
    `unsupported protocol version ${e.protocolVersion}`,
  );
}

function validateHandshake(e: DecodedEnvelope): void {
  const handshakeKind =
    e.messageKind === MessageKind.Capabilities || e.messageKind === MessageKind.Authenticated;
  const handshakeUnion =
    e.controlType === ControlPayload.CapabilitiesPayload ||
    e.controlType === ControlPayload.AuthenticatedPayload;
  if (!handshakeKind && !handshakeUnion) return;

  requireSemantics(
    e.deliveryClass === DeliveryClass.ReliableOrdered &&
      isConnectionScoped(e) &&
      hasNoDomainPayload(e),
    "invalid handshake envelope scope, delivery, or domain payload",
  );
  if (e.messageKind === MessageKind.Capabilities) {
    if (
      e.controlType !== ControlPayload.CapabilitiesPayload ||
      !(e.control instanceof CapabilitiesPayload)
    ) {
      throw new CodecError(
        "InvalidSemantics",
        "Capabilities message must carry CapabilitiesPayload",
      );
    }
    const capabilities = e.control;
    requireSemantics(
      capabilities.selectedProtocolVersion() === PROTOCOL_VERSION,
      "Capabilities must select protocol v1",
    );
    requireSemantics(
      capabilities.maxFrameSize() > 0 &&
        capabilities.maxPayloadSize() > 0 &&
        capabilities.maxPayloadSize() <= capabilities.maxFrameSize(),
      "Capabilities advertised limits must be positive and payload-bounded by frame size",
    );
  } else {
    if (
      e.controlType !== ControlPayload.AuthenticatedPayload ||
      !(e.control instanceof AuthenticatedPayload)
    ) {
      throw new CodecError(
        "InvalidSemantics",
        "Authenticated message must carry AuthenticatedPayload",
      );
    }
    requireSemantics(
      e.control.principalId() !== 0n,
      "Authenticated principal ID must be nonzero",
    );
  }
}

function validateProtocolError(e: DecodedEnvelope): void {
  const errorKind = e.messageKind === MessageKind.ProtocolError;
  const errorUnion = e.controlType === ControlPayload.ProtocolErrorPayload;
  if (!errorKind && !errorUnion) return;
  if (!errorKind || !errorUnion || !(e.control instanceof ProtocolErrorPayload)) {
    throw new CodecError(
      "InvalidSemantics",
      "ProtocolError message must carry ProtocolErrorPayload",
    );
  }
  requireSemantics(
    e.deliveryClass === DeliveryClass.ReliableOrdered && hasNoDomainPayload(e),
    "invalid ProtocolError delivery or domain payload",
  );
  requireSemantics(
    e.control.code() !== ProtocolErrorCode.Unknown &&
      e.control.relatedMessageKind() !== MessageKind.Unknown,
    "ProtocolError code and related message kind must be known",
  );
  const hasSpaceDetail =
    e.spaceId !== 0n || e.spaceEpoch !== 0n || e.channelId !== null || e.entityId !== null;
  requireSemantics(
    !(e.namespaceId === 0n && (e.sessionId !== 0n || hasSpaceDetail)) &&
      !(e.sessionId === 0n && hasSpaceDetail) &&
      !(e.spaceId === 0n && (e.spaceEpoch !== 0n || e.channelId !== null || e.entityId !== null)),
    "invalid ProtocolError optional scope",
  );
}

/** Enforce managed-admission wire semantics before exposing an envelope to callers. */
function validateManaged(e: DecodedEnvelope): void {
  const kindManaged = e.messageKind >= MessageKind.RequestAdmission && e.messageKind <= MessageKind.QueueUpdate;
  const unionManaged = e.controlType >= ControlPayload.RequestAdmissionPayload && e.controlType <= ControlPayload.QueueUpdatePayload;
  if (!kindManaged && !unionManaged) return;
  const require = (valid: boolean): void => {
    requireSemantics(valid, "invalid managed admission envelope or fields");
  };
  require(kindManaged && unionManaged && e.messageKind - e.controlType === 3);
  require(e.protocolVersion === PROTOCOL_VERSION && e.deliveryClass === DeliveryClass.ReliableOrdered);
  require(e.namespaceId !== 0n && e.sessionId !== 0n && e.correlationId !== null);
  require(e.spaceId === 0n && e.spaceEpoch === 0n && e.channelId === null && e.entityId === null);
  require(e.payloadTypeId === 0n && (e.payload === null || e.payload.length === 0));
  const c = e.control;
  if (c instanceof RequestAdmissionPayload) {
    const length = new TextEncoder().encode(c.idempotencyKey() ?? "").length;
    require(length > 0 && length <= 256);
  } else if (c instanceof AdmissionResultPayload) {
    const queued = c.status() === AdmissionStatus.Queued;
    require(c.status() >= AdmissionStatus.Admitted && c.status() <= AdmissionStatus.Rejected);
    require(c.rejectionCode() >= AdmissionRejectionCode.None && c.rejectionCode() <= AdmissionRejectionCode.InvalidIdempotencyKey);
    require((c.status() === AdmissionStatus.Rejected) === (c.rejectionCode() !== AdmissionRejectionCode.None));
    require(queued === (c.ticketId() !== 0n));
    require(c.pollAfterMs() <= 30_000 && c.ticketRemainingMs() <= 900_000);
    require(queued || c.ticketRemainingMs() === 0);
    require(queued || c.status() === AdmissionStatus.Paused || c.pollAfterMs() === 0);
  } else if (c instanceof QueueUpdatePayload) {
    const live = c.state() === QueueState.Waiting || c.state() === QueueState.Offered;
    require(c.ticketId() !== 0n && c.state() >= QueueState.Waiting && c.state() <= QueueState.Missing);
    require((c.state() === QueueState.Waiting) === (c.position() !== 0));
    require(c.pollAfterMs() <= 30_000 && c.ticketRemainingMs() <= 900_000 && c.offerRemainingMs() <= 30_000);
    require(c.state() === QueueState.Offered || c.offerRemainingMs() === 0);
    require(live || (c.pollAfterMs() === 0 && c.ticketRemainingMs() === 0));
  } else if (c instanceof QueueStatusRequestPayload || c instanceof QueueHeartbeatPayload || c instanceof QueueClaimPayload || c instanceof QueueCancelPayload) {
    require(c.ticketId() !== 0n);
  } else {
    require(false);
  }
}
