import * as flatbuffers from "flatbuffers";
import {
  Envelope as FbEnvelope,
  DeliveryClass,
  MessageKind,
  ControlPayload,
  RequestAdmissionPayload, AdmissionResultPayload, AdmissionStatus, AdmissionRejectionCode,
  QueueStatusRequestPayload, QueueHeartbeatPayload, QueueClaimPayload, QueueCancelPayload,
  QueueUpdatePayload, QueueState,
} from "../generated/woven/protocol/v1.js";
import { unionToControlPayload } from "../generated/woven/protocol/v1/control-payload.js";

export const PROTOCOL_VERSION = 1;
export const FILE_IDENTIFIER = "WVN1";

/** Minimum frame length: 4-byte size prefix + root table offset. */
const MIN_FRAME_LENGTH = 12;

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
  /** Decode a full size-prefixed frame into a {@link DecodedEnvelope}. */
  decode(frame: Uint8Array): DecodedEnvelope {
    if (frame.length < MIN_FRAME_LENGTH) {
      throw new CodecError(
        "TruncatedFrame",
        `frame too short: ${frame.length} bytes`,
      );
    }
    const prefix = readUint32LE(frame, 0);
    if (prefix < 8) {
      throw new CodecError("InvalidSizePrefix", `invalid size prefix ${prefix}`);
    }
    if (prefix !== frame.length - 4) {
      throw new CodecError(
        "TruncatedFrame",
        `expected ${prefix + 4} bytes frame, got ${frame.length}`,
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
    const bb = new flatbuffers.ByteBuffer(
      new Uint8Array(frame.buffer, frame.byteOffset, frame.length),
    );
    const envelope = FbEnvelope.getSizePrefixedRootAsEnvelope(bb);
    return decodeEnvelope(envelope);
  }

  /**
   * Handle one frame from a byte stream given the accumulated buffer. Returns
   * the decoded envelope and how many bytes were consumed, or `null` when more
   * bytes are required to complete a frame.
   */
  decodeStream(
    acc: Uint8Array,
  ): { envelope: DecodedEnvelope; consumed: number } | null {
    if (acc.length < 4) return null;
    const prefix = readUint32LE(acc, 0);
    if (prefix < 8) throw new CodecError("InvalidSizePrefix", `invalid size prefix ${prefix}`);
    const frameLength = prefix + 4;
    if (acc.length < frameLength) return null;
    const frame = acc.subarray(0, frameLength);
    return { envelope: this.decode(frame), consumed: frameLength };
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
  validateManaged(decoded);
  return decoded;
}

/** Managed controls are codec-compatible only; this does not enable a managed browser transport. */
function validateManaged(e: DecodedEnvelope): void {
  const kindManaged = e.messageKind >= MessageKind.RequestAdmission && e.messageKind <= MessageKind.QueueUpdate;
  const unionManaged = e.controlType >= ControlPayload.RequestAdmissionPayload && e.controlType <= ControlPayload.QueueUpdatePayload;
  if (!kindManaged && !unionManaged) return;
  const require = (valid: boolean): void => {
    if (!valid) throw new CodecError("InvalidSemantics", "invalid managed admission envelope or fields");
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
