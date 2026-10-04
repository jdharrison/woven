import {
  CAPABILITY_POSITIONED_ENTITY_STATE,
  CAPABILITY_CLIENT_LOG,
  CodecError,
  DEFAULT_MAX_FRAME_BYTES,
  DEFAULT_MAX_PAYLOAD_BYTES,
  EnvelopeCodec,
  DecodedEnvelope,
  PROTOCOL_VERSION,
  RoutingPosition3D,
  validatePayloadLength,
  validateUnreliableEntityState,
  validateLogMessage,
} from "./codec.js";
import {
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
import {
  WebTransport,
  WebTransportBidirectionalStream,
  WebTransportDatagramDuplexStream,
  WebTransportOptions,
  normalizeWebTransportOptions,
  resolveWebTransportConstructor,
} from "./webtransport.js";
import { toWebTransportUrl } from "./url.js";
import {
  AdmissionRejectionCode,
  AdmissionResultPayload,
  AdmissionStatus,
  AuthenticatedPayload,
  AuthenticationScheme,
  CapabilitiesPayload,
  ControlPayload,
  DeliveryClass,
  MessageKind,
  ProtocolErrorPayload,
  ProtocolErrorCode,
  LogLevel,
  QueueState,
  QueueUpdatePayload,
} from "../generated/woven/protocol/v1.js";

/** Client connection settings. */
export interface WovenConfig {
  /**
   * Server URL: `quic://host:port` (the standardized scheme), or an explicit
   * WebTransport URL `https://host:port/webtransport` / `wtransport://host:port/path`.
   *
   * A browser client only speaks WebTransport, so a `quic://` URL is resolved
   * to WebTransport using the deterministic port convention: WebTransport
   * lives one port above the QUIC port (`quic://host:P` → `host:(P+1)` on the
   * `/webtransport` path). Native clients use the `quic://` URL directly.
   */
  url: string;
  /** Opaque credential sent during `Authenticate` under the selected scheme. */
  token: string;
  /** Maximum incoming frame size advertised in `Hello` (bytes). Defaults to 1 MiB. */
  maxFrameBytes?: number;
  /** Incoming payload limit in bytes (default 64 KiB); smaller values also limit publishing. */
  maxPayloadBytes?: number;
  /** WVN1 authentication scheme. Defaults to Development for existing local deployments. */
  authenticationScheme?: AuthenticationScheme;
  /** Safe options forwarded to the WHATWG WebTransport constructor. */
  webTransportOptions?: WebTransportOptions;
  /** Total milliseconds allowed for readiness, stream creation, and the WVN1 handshake. */
  connectTimeoutMs?: number;
  /** Optional incoming/outgoing datagram queue expiry in milliseconds; default null (no expiry). */
  datagramMaxAgeMs?: number;
}

/** Normalized result of a managed admission request. */
export interface AdmissionResult {
  status: AdmissionStatus;
  rejectionCode: AdmissionRejectionCode;
  ticketId: bigint | null;
  pollAfterMs: number;
  ticketRemainingMs: number;
}

/** Normalized result of a managed queue operation. */
export interface QueueUpdate {
  ticketId: bigint;
  state: QueueState;
  position: number;
  pollAfterMs: number;
  ticketRemainingMs: number;
  offerRemainingMs: number;
}

/** A semantic admission result is returned as data and is never automatically retried. */
export type ManagedAdmissionOutcome =
  | { kind: "admission"; result: AdmissionResult }
  | { kind: "queue"; update: QueueUpdate };

/** Session logs are sent once; completion is not a persistence acknowledgement. */
export interface ClientLogger {
  info(message: string): Promise<void>;
  warn(message: string): Promise<void>;
  error(message: string): Promise<void>;
}

/** Errors surfaced by the Woven TypeScript WebTransport client. */
export type WovenError =
  | { kind: "transport"; message: string }
  | { kind: "protocol"; message: string }
  | { kind: "server"; message: string }
  | { kind: "handshake"; message: string }
  | { kind: "closed"; message: string };

function err(kind: WovenError["kind"], message: string): WovenError {
  return { kind, message };
}

function isProtocolError(envelope: DecodedEnvelope): boolean {
  return envelope.messageKind === MessageKind.ProtocolError;
}

const ADMISSION_EXCHANGE_TIMEOUT_MS = 10_000;
const MAX_ADMISSION_WAIT_MS = 15 * 60 * 1_000;
const MAX_GRACEFUL_CLOSE_TIMEOUT_MS = 10_000;
const MAX_PENDING_ENVELOPES = 64;
const DATAGRAM_INCOMING_HIGH_WATER_MARK = 8;
const DATAGRAM_OUTGOING_HIGH_WATER_MARK = 1;

type Lifecycle = "handshaking" | "ready" | "closed";

type DatagramOutcome =
  | { ok: true; envelope: DecodedEnvelope | null }
  | { ok: false; error: unknown };

type PendingDatagramReceive = {
  promise: Promise<DecodedEnvelope | null>;
  outcome: DatagramOutcome | null;
  notify: (() => void) | null;
};

type ClientLimits = {
  maxFrameBytes: number;
  maxPayloadBytes: number;
};

/**
 * Woven WebTransport client for browsers.
 *
 * Mirrors the Rust reference client's `Client` API over the WHATWG
 * `WebTransport` transport: it opens one client-initiated bidirectional stream,
 * completes the `Hello → Capabilities → Authenticate → Authenticated` handshake,
 * and exposes methods for join/subscribe/publish/drain. Reliable envelopes arrive
 * on the control stream; unreliable datagrams arrive on the datagram channel.
 */
export class WovenClient {
  readonly transport: WebTransport;
  readonly stream: WebTransportBidirectionalStream;
  readonly logger: ClientLogger = {
    info: (message) => this.sendLog(LogLevel.Info, message),
    warn: (message) => this.sendLog(LogLevel.Warn, message),
    error: (message) => this.sendLog(LogLevel.Error, message),
  };
  private sessionScope: { namespaceId: bigint; sessionId: bigint } | null = null;
  private readonly codec: EnvelopeCodec;
  private maxPublishPayloadBytes: number;
  private maxPublishFrameBytes: number;
  private negotiatedCapabilityBits = 0n;
  private datagramLane: WebTransportDatagramDuplexStream | null = null;
  private datagramReader: ReadableStreamDefaultReader<Uint8Array> | null = null;
  private datagramWriter: WritableStreamDefaultWriter<Uint8Array> | null = null;
  private datagramReceiveActive = false;
  private datagramReadActive = false;
  private datagramWriteActive = false;
  private datagramEnded = false;
  private pendingDatagramReceive: PendingDatagramReceive | null = null;
  private stopDatagramRead: (() => void) | null = null;
  private stopDatagramWrite: (() => void) | null = null;
  private inBuffer: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  private pending: DecodedEnvelope[] = [];
  private lifecycle: Lifecycle = "handshaking";
  private readActive = false;
  private writeActive = false;
  private admissionExchangeActive = false;
  private admissionRunnerActive = false;
  private deferredReceive: Promise<DecodedEnvelope> | null = null;
  private closeInitiated = false;

  private constructor(
    transport: WebTransport,
    stream: WebTransportBidirectionalStream,
    codec: EnvelopeCodec,
  ) {
    this.transport = transport;
    this.stream = stream;
    this.codec = codec;
    this.maxPublishPayloadBytes = Math.min(DEFAULT_MAX_PAYLOAD_BYTES, codec.maxPayloadBytes);
    this.maxPublishFrameBytes = codec.maxFrameBytes;
    // One lifecycle observer, not one handler per receive or a background draining loop.
    void transport.closed.then(
      () => { this.sessionScope = null; this.stopDatagramLane(); },
      () => { this.sessionScope = null; this.stopDatagramLane(); },
    );
  }

  /**
   * Connect to a Woven server over WebTransport and complete the protocol
   * handshake.
   */
  static async connect(
    config: WovenConfig,
  ): Promise<WovenClient> {
    const connectTimeoutMs = validatedConnectionTimeout(config.connectTimeoutMs);
    const limits = validatedClientLimits(config);
    validateDatagramMaxAge(config.datagramMaxAgeMs);
    const WebTransportCtor = resolveWebTransportConstructor();
    const url = toWebTransportUrl(config.url);
    const deadline = connectionNow() + connectTimeoutMs;

    const options = normalizeWebTransportOptions(config.webTransportOptions);
    const transport = new WebTransportCtor(url, options);
    try {
      await withTimeout(
        transport.ready,
        remainingConnectionTime(deadline, "WebTransport connect timed out"),
        "WebTransport connect timed out",
      );
      const stream = await withTimeout(
        transport.createBidirectionalStream(),
        remainingConnectionTime(deadline, "WebTransport stream creation timed out"),
        "WebTransport stream creation timed out",
      );
      return await WovenClient.completeHandshake(
        transport,
        stream,
        config,
        limits,
        deadline,
        false,
      );
    } catch (error) {
      transport.close();
      throw error;
    }
  }

  /**
   * Build a client around an already-created WebTransport session and complete
   * the protocol handshake. Exposed for embedding and test harnesses that
   * manage the transport themselves.
   */
  static async fromTransport(
    transport: WebTransport,
    stream: WebTransportBidirectionalStream,
    config: WovenConfig,
  ): Promise<WovenClient> {
    const timeoutMs = validatedConnectionTimeout(config.connectTimeoutMs);
    const limits = validatedClientLimits(config);
    validateDatagramMaxAge(config.datagramMaxAgeMs);
    return WovenClient.completeHandshake(
      transport,
      stream,
      config,
      limits,
      connectionNow() + timeoutMs,
      true,
    );
  }

  private static async completeHandshake(
    transport: WebTransport,
    stream: WebTransportBidirectionalStream,
    config: WovenConfig,
    limits: ClientLimits,
    deadline: number,
    closeOnError: boolean,
  ): Promise<WovenClient> {
    const client = new WovenClient(
      transport,
      stream,
      new EnvelopeCodec(limits.maxFrameBytes, limits.maxPayloadBytes),
    );
    try {
      const timeoutMs = remainingConnectionTime(deadline, "WVN1 handshake timed out");
      await withTimeout(client.handshake(config), timeoutMs, "WVN1 handshake timed out");
      client.configureDatagramLane(config);
      client.lifecycle = "ready";
      return client;
    } catch (error) {
      if (closeOnError) client.shutdown("handshake failed");
      if (error instanceof CodecError) throw err("protocol", error.message);
      throw error;
    }
  }

  /** Send a `JoinSession` control envelope. */
  async joinSession(namespaceId: bigint, sessionId: bigint): Promise<void> {
    await this.write(encodeJoinSession({ namespaceId, sessionId }));
    if (this.lifecycle === "ready") this.sessionScope = { namespaceId, sessionId };
  }

  /** Leave the remembered session and disable logging until another join/admission. */
  async leaveSession(reason = "client request"): Promise<void> {
    this.assertReady();
    if (this.sessionScope === null) throw err("protocol", "no joined or admitted session");
    const frame = encodeLeaveSession(this.sessionScope, reason);
    await this.write(frame);
    this.sessionScope = null;
  }

  /** Info-level convenience alias; sends only, without waiting for server persistence. */
  async log(message: string): Promise<void> {
    await this.logger.info(message);
  }

  private async sendLog(level: LogLevel, message: string): Promise<void> {
    this.assertReady();
    if (this.sessionScope === null) throw err("protocol", "no joined or admitted session");
    if ((this.negotiatedCapabilityBits & CAPABILITY_CLIENT_LOG) === 0n) {
      throw err("protocol", "server did not negotiate ClientLog");
    }
    try {
      validatePayloadLength(validateLogMessage(level, message), this.maxPublishPayloadBytes);
      const frame = encodeClientLog(this.sessionScope, level, message);
      if (frame.byteLength > this.maxPublishFrameBytes) {
        throw new CodecError("FrameTooLarge", `log frame exceeds limit ${this.maxPublishFrameBytes}`);
      }
      await this.write(frame);
    } catch (error) {
      if (error instanceof CodecError) throw err("protocol", error.message);
      throw error;
    }
  }

  /** Request managed admission and atomic session join. */
  async requestAdmission(
    namespaceId: bigint,
    sessionId: bigint,
    correlationId: bigint,
    idempotencyKey: string,
  ): Promise<AdmissionResult> {
    return this.requestAdmissionInternal(
      namespaceId,
      sessionId,
      correlationId,
      idempotencyKey,
      undefined,
      false,
    );
  }

  /** Observe a ticket without renewing its heartbeat. */
  async queueStatus(
    namespaceId: bigint,
    sessionId: bigint,
    correlationId: bigint,
    ticketId: bigint,
  ): Promise<QueueUpdate> {
    return this.queueExchange(
      namespaceId,
      sessionId,
      correlationId,
      ticketId,
      MessageKind.QueueStatusRequest,
      encodeQueueStatusRequest,
      undefined,
      false,
    );
  }

  /** Renew the connection-owned ticket heartbeat and observe current status. */
  async queueHeartbeat(
    namespaceId: bigint,
    sessionId: bigint,
    correlationId: bigint,
    ticketId: bigint,
  ): Promise<QueueUpdate> {
    return this.queueExchange(
      namespaceId,
      sessionId,
      correlationId,
      ticketId,
      MessageKind.QueueHeartbeat,
      encodeQueueHeartbeat,
      undefined,
      false,
    );
  }

  /** Claim an offer; Admitted means the server has already joined this connection. */
  async queueClaim(
    namespaceId: bigint,
    sessionId: bigint,
    correlationId: bigint,
    ticketId: bigint,
  ): Promise<QueueUpdate> {
    return this.queueExchange(
      namespaceId,
      sessionId,
      correlationId,
      ticketId,
      MessageKind.QueueClaim,
      encodeQueueClaim,
      undefined,
      false,
    );
  }

  /** Cancel a ticket without undoing an already-admitted session join. */
  async queueCancel(
    namespaceId: bigint,
    sessionId: bigint,
    correlationId: bigint,
    ticketId: bigint,
  ): Promise<QueueUpdate> {
    return this.queueExchange(
      namespaceId,
      sessionId,
      correlationId,
      ticketId,
      MessageKind.QueueCancel,
      encodeQueueCancel,
      undefined,
      false,
    );
  }

  /**
   * Wait for managed admission with bounded polling and caller-owned cancellation.
   * The connection is closed on cancellation, timeout, transport failure, or protocol mismatch.
   */
  async admitWithCancellation(
    namespaceId: bigint,
    sessionId: bigint,
    idempotencyKey: string,
    timeoutMs: number,
    signal: AbortSignal,
  ): Promise<ManagedAdmissionOutcome> {
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0 || timeoutMs > MAX_ADMISSION_WAIT_MS) {
      throw err("transport", "admission deadline must be positive and at most 15 minutes");
    }
    this.assertReady();
    if (signal.aborted) {
      this.shutdown("admission stopped");
      throw err("transport", "admission cancelled; connection closed");
    }
    if (
      this.admissionRunnerActive ||
      this.admissionExchangeActive ||
      this.readActive ||
      this.writeActive ||
      this.deferredReceive !== null ||
      this.hasDatagramOperation()
    ) {
      throw err("transport", "client already has an active stream or admission operation");
    }

    this.admissionRunnerActive = true;
    const controller = new AbortController();
    const cancel = (): void => controller.abort("cancelled");
    if (signal.aborted) cancel();
    else signal.addEventListener("abort", cancel, { once: true });
    const deadline = setTimeout(() => controller.abort("deadline"), timeoutMs);

    try {
      return await this.waitAdmission(
        namespaceId,
        sessionId,
        idempotencyKey,
        controller.signal,
      );
    } catch (error) {
      this.shutdown("admission stopped");
      throw error;
    } finally {
      clearTimeout(deadline);
      signal.removeEventListener("abort", cancel);
      this.admissionRunnerActive = false;
    }
  }

  /** Send a `SubscribeSpace` control envelope. */
  async subscribeSpace(
    namespaceId: bigint,
    sessionId: bigint,
    spaceId: bigint,
    spaceEpoch: bigint,
    channelId: bigint,
  ): Promise<void> {
    await this.write(
      encodeSubscribeSpace({
        namespaceId,
        sessionId,
        spaceId,
        spaceEpoch,
        channelId,
      }),
    );
  }

  /** Move an entity between two subscribed spaces. */
  async transitionEntity(
    namespaceId: bigint,
    sessionId: bigint,
    sourceSpaceId: bigint,
    sourceEpoch: bigint,
    destinationSpaceId: bigint,
    destinationEpoch: bigint,
    entityId: bigint,
  ): Promise<void> {
    await this.write(
      encodeSpaceTransition(
        {
          namespaceId,
          sessionId,
          spaceId: sourceSpaceId,
          spaceEpoch: sourceEpoch,
          entityId,
        },
        {
          fromSpaceId: sourceSpaceId,
          toSpaceId: destinationSpaceId,
          toSpaceEpoch: destinationEpoch,
        },
      ),
    );
  }

  /** Request a scoped opaque snapshot from the server. */
  async requestSnapshot(
    namespaceId: bigint,
    sessionId: bigint,
    spaceId: bigint,
    spaceEpoch: bigint,
    channelId: bigint,
    afterServerTick?: bigint,
  ): Promise<void> {
    await this.write(
      encodeSnapshotRequest(
        { namespaceId, sessionId, spaceId, spaceEpoch, channelId },
        afterServerTick,
      ),
    );
  }

  /**
   * Send a reliable event opaque payload envelope (at most 64 KiB, or a smaller configured/server cap).
   *
   * `sequence` must be strictly monotone per connection × space × epoch × entity × channel.
   */
  async publishEvent(
    namespaceId: bigint,
    sessionId: bigint,
    spaceId: bigint,
    spaceEpoch: bigint,
    channelId: bigint,
    entityId: bigint,
    sequence: bigint,
    typeId: bigint,
    payload: Uint8Array,
  ): Promise<void> {
    this.validatePublishPayload(payload);
    await this.write(
      encodeReliableEvent(
        {
          namespaceId,
          sessionId,
          spaceId,
          spaceEpoch,
          channelId,
          entityId,
          senderSequence: sequence,
        },
        { typeId, bytes: payload },
      ),
    );
  }

  /**
   * Send a latest-value (entity state) opaque payload envelope (at most 64 KiB, or a smaller configured/server cap).
   *
   * `sequence` must be strictly monotone per connection × space × epoch × entity × channel.
   */
  async publishState(
    namespaceId: bigint,
    sessionId: bigint,
    spaceId: bigint,
    spaceEpoch: bigint,
    channelId: bigint,
    entityId: bigint,
    sequence: bigint,
    typeId: bigint,
    payload: Uint8Array,
  ): Promise<void> {
    this.validatePublishPayload(payload);
    await this.write(
      encodeEntityState(
        {
          namespaceId,
          sessionId,
          spaceId,
          spaceEpoch,
          channelId,
          entityId,
          senderSequence: sequence,
        },
        { typeId, bytes: payload },
      ),
    );
  }

  /** Publish state with a 3D routing position applied atomically by the server. */
  async publishPositionedState(
    namespaceId: bigint,
    sessionId: bigint,
    spaceId: bigint,
    spaceEpoch: bigint,
    channelId: bigint,
    entityId: bigint,
    sequence: bigint,
    typeId: bigint,
    position: RoutingPosition3D,
    payload: Uint8Array,
  ): Promise<void> {
    this.requirePositionedState();
    this.validatePublishPayload(payload);
    await this.write(
      encodeEntityState(
        {
          namespaceId,
          sessionId,
          spaceId,
          spaceEpoch,
          channelId,
          entityId,
          senderSequence: sequence,
          routingPosition: position,
        },
        { typeId, bytes: payload },
      ),
    );
  }

  /**
   * Queue one opaque EntityState / UnreliableSequenced datagram. No fragmentation,
   * retry, reliable fallback, or delivery acknowledgement. Sequence ownership stays with the caller.
   */
  async publishUnreliableState(
    namespaceId: bigint,
    sessionId: bigint,
    spaceId: bigint,
    spaceEpoch: bigint,
    channelId: bigint,
    entityId: bigint,
    sequence: bigint,
    typeId: bigint,
    payload: Uint8Array,
  ): Promise<void> {
    const lane = this.assertDatagramLane();
    this.validatePublishPayload(payload);
    if (this.datagramWriteActive) {
      throw err("transport", "another datagram write is already active");
    }
    try {
      const frame = encodeUnreliableEntityState(
        { namespaceId, sessionId, spaceId, spaceEpoch, channelId, entityId, senderSequence: sequence },
        { typeId, bytes: payload },
      );
      if (frame.byteLength > this.maxPublishFrameBytes) {
        throw err(
          "protocol",
          `encoded datagram frame length ${frame.byteLength} bytes exceeds frame limit ${this.maxPublishFrameBytes} bytes`,
        );
      }
      // The transport MTU is dynamic; check the complete frame immediately before writing.
      if (frame.byteLength > lane.maxDatagramSize) {
        throw err(
          "transport",
          `encoded datagram frame length ${frame.byteLength} bytes exceeds maxDatagramSize ${lane.maxDatagramSize} bytes`,
        );
      }
      await this.writeDatagram(frame, lane);
    } catch (error) {
      if (error instanceof CodecError) throw err("protocol", error.message);
      throw error;
    }
  }

  /** Queue positioned unreliable state without retry or reliable fallback. */
  async publishUnreliablePositionedState(
    namespaceId: bigint,
    sessionId: bigint,
    spaceId: bigint,
    spaceEpoch: bigint,
    channelId: bigint,
    entityId: bigint,
    sequence: bigint,
    typeId: bigint,
    position: RoutingPosition3D,
    payload: Uint8Array,
  ): Promise<void> {
    this.requirePositionedState();
    const lane = this.assertDatagramLane();
    this.validatePublishPayload(payload);
    if (this.datagramWriteActive) {
      throw err("transport", "another datagram write is already active");
    }
    try {
      const frame = encodeUnreliableEntityState(
        {
          namespaceId,
          sessionId,
          spaceId,
          spaceEpoch,
          channelId,
          entityId,
          senderSequence: sequence,
          routingPosition: position,
        },
        { typeId, bytes: payload },
      );
      if (frame.byteLength > this.maxPublishFrameBytes) {
        throw err(
          "protocol",
          `encoded datagram frame length ${frame.byteLength} bytes exceeds frame limit ${this.maxPublishFrameBytes} bytes`,
        );
      }
      if (frame.byteLength > lane.maxDatagramSize) {
        throw err(
          "transport",
          `encoded datagram frame length ${frame.byteLength} bytes exceeds maxDatagramSize ${lane.maxDatagramSize} bytes`,
        );
      }
      await this.writeDatagram(frame, lane);
    } catch (error) {
      if (error instanceof CodecError) throw err("protocol", error.message);
      throw error;
    }
  }

  /** Whether the handshake negotiated atomic positioned EntityState support. */
  supportsPositionedState(): boolean {
    return (this.negotiatedCapabilityBits & CAPABILITY_POSITIONED_ENTITY_STATE) !== 0n;
  }

  /** Send an `InferenceRequested` control envelope addressed to the AI identity's entity. */
  async requestInference(
    namespaceId: bigint,
    sessionId: bigint,
    spaceId: bigint,
    spaceEpoch: bigint,
    aiEntityId: bigint,
    capability: string,
    deadlineMs: bigint,
    input: Uint8Array,
  ): Promise<void> {
    try {
      await this.write(
        encodeInferenceRequested(
          { namespaceId, sessionId, spaceId, spaceEpoch, entityId: aiEntityId },
          { capability, deadlineMs, input },
        ),
      );
    } catch (error) {
      if (error instanceof CodecError) throw err("protocol", error.message);
      throw error;
    }
  }

  /**
   * Receive the next decoded envelope from the control stream, blocking until
   * one arrives.
   */
  async recv(): Promise<DecodedEnvelope> {
    this.assertReady();
    if (this.admissionRunnerActive || this.admissionExchangeActive) {
      throw err("transport", "admission operation owns the control-stream reader");
    }
    return this.takeOrBeginReceive();
  }

  /** Try to receive the next envelope within `timeoutMs`. Returns `null` on timeout. */
  async recvTimeout(timeoutMs: number): Promise<DecodedEnvelope | null> {
    this.assertReady();
    if (this.admissionRunnerActive || this.admissionExchangeActive) {
      throw err("transport", "admission operation owns the control-stream reader");
    }
    const receive = this.takeOrBeginReceive();
    return withTimeout(receive, timeoutMs, undefined).then(
      (envelope) => envelope,
      (error) => {
        if (error && (error as { name?: string }).name === "TimeoutError") {
          this.deferredReceive = receive;
          return null;
        }
        throw error;
      },
    );
  }

  /**
   * Receive one opaque unreliable EntityState independently of recv(). Returns null on lane end.
   * Malformed datagrams consume one packet and reject with kind "protocol"; no ordering is added.
   */
  async recvDatagram(): Promise<DecodedEnvelope | null> {
    const lane = this.assertDatagramLane();
    this.beginDatagramReceive();
    try {
      return await this.waitForDatagramReceive(this.takeOrBeginDatagramReceive(lane));
    } finally {
      this.datagramReceiveActive = false;
    }
  }

  /** Return null on timeout/lane end, retaining at most one read for the next datagram receive. */
  async recvDatagramTimeout(timeoutMs: number): Promise<DecodedEnvelope | null> {
    const lane = this.assertDatagramLane();
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0 || timeoutMs > MAX_TIMER_TIMEOUT_MS) {
      throw err(
        "transport",
        `datagram timeout must be finite and between 0 and ${MAX_TIMER_TIMEOUT_MS}ms`,
      );
    }
    this.beginDatagramReceive();
    try {
      return await this.waitForDatagramReceive(this.takeOrBeginDatagramReceive(lane), timeoutMs);
    } finally {
      this.datagramReceiveActive = false;
    }
  }

  /** Initiate WebTransport session closure without waiting for transport shutdown. */
  close(closeCode = 0, reason = "client closed"): void {
    if (this.closeInitiated) return;
    this.initiateClose(closeCode, reason);
  }

  /** Close the WebTransport session and wait a bounded time for transport shutdown. */
  async closeGracefully(
    timeoutMs = 2_000,
    closeCode = 0,
    reason = "client closed",
  ): Promise<void> {
    const timeout = validatedGracefulCloseTimeout(timeoutMs);
    try {
      if (!this.closeInitiated) this.initiateClose(closeCode, reason);
      await withTimeout(this.transport.closed, timeout, "WebTransport close timed out");
    } catch (error) {
      if (isWovenError(error)) throw error;
      if (error && (error as { name?: string }).name === "TimeoutError") {
        throw err("transport", `WebTransport close timed out after ${timeout}ms`);
      }
      throw err(
        "transport",
        `WebTransport close failed: ${runtimeErrorMessage(error, "unknown transport error")}`,
      );
    }
  }

  private async handshake(config: WovenConfig): Promise<void> {
    const limits = validatedClientLimits(config);
    await this.writeRaw(
      encodeHello({
        clientName: "woven-client-ts",
        clientVersion: "0.3.0",
        maxFrameSize: limits.maxFrameBytes,
        maxPayloadSize: limits.maxPayloadBytes,
      }),
    );
    const capabilities = await this.expectKindDuringHandshake(
      MessageKind.Capabilities,
      MessageKind.Hello,
    );
    if (
      capabilities.controlType !== ControlPayload.CapabilitiesPayload ||
      !(capabilities.control instanceof CapabilitiesPayload)
    ) {
      throw err("handshake", "Capabilities response has the wrong control payload");
    }
    if (
      capabilities.control.selectedProtocolVersion() !== PROTOCOL_VERSION ||
      capabilities.control.maxFrameSize() <= 0 ||
      capabilities.control.maxPayloadSize() <= 0 ||
      capabilities.control.maxPayloadSize() > capabilities.control.maxFrameSize()
    ) {
      throw err("handshake", "Capabilities response has invalid protocol version or limits");
    }
    this.maxPublishPayloadBytes = Math.min(
      this.maxPublishPayloadBytes,
      capabilities.control.maxPayloadSize(),
    );
    this.maxPublishFrameBytes = Math.min(
      this.maxPublishFrameBytes,
      capabilities.control.maxFrameSize(),
    );
    this.negotiatedCapabilityBits = capabilities.control.capabilityBits();

    await this.writeRaw(
      encodeAuthenticate(
        new TextEncoder().encode(config.token),
        config.authenticationScheme ?? AuthenticationScheme.Development,
      ),
    );
    const authenticated = await this.expectKindDuringHandshake(
      MessageKind.Authenticated,
      MessageKind.Authenticate,
    );
    if (
      authenticated.controlType !== ControlPayload.AuthenticatedPayload ||
      !(authenticated.control instanceof AuthenticatedPayload) ||
      authenticated.control.principalId() === 0n
    ) {
      throw err("handshake", "Authenticated response has invalid payload or principal ID");
    }
  }

  private async expectKindDuringHandshake(
    kind: MessageKind,
    relatedRequestKind: MessageKind,
  ): Promise<DecodedEnvelope> {
    const envelope = await this.receiveExclusive();
    if (isProtocolError(envelope)) {
      const errorPayload = currentProtocolError(
        envelope,
        relatedRequestKind,
        0n,
        0n,
        null,
      );
      if (errorPayload === null) {
        throw err("handshake", "ProtocolError does not match the active handshake operation");
      }
      throw err("server", errorPayload.message() ?? "server rejected handshake operation");
    }
    if (envelope.messageKind !== kind) {
      throw err("handshake", `expected ${kindName(kind)}, got ${kindName(envelope.messageKind)}`);
    }
    if (!isHandshakeEnvelope(envelope)) {
      throw err("handshake", `${kindName(kind)} response has invalid envelope semantics`);
    }
    return envelope;
  }

  private async requestAdmissionInternal(
    namespaceId: bigint,
    sessionId: bigint,
    correlationId: bigint,
    idempotencyKey: string,
    signal: AbortSignal | undefined,
    runnerOwned: boolean,
  ): Promise<AdmissionResult> {
    const frame = encodeRequestAdmission(
      { namespaceId, sessionId, correlationId },
      idempotencyKey,
    );
    const reply = await this.admissionExchange(
      frame,
      namespaceId,
      sessionId,
      correlationId,
      MessageKind.RequestAdmission,
      MessageKind.AdmissionResult,
      signal,
      runnerOwned,
    );
    if (!(reply.control instanceof AdmissionResultPayload)) {
      this.shutdown("admission protocol mismatch");
      throw err("protocol", "admission reply payload mismatch");
    }
    const result = normalizeAdmissionResult(reply.control);
    if (result.status === AdmissionStatus.Admitted && this.lifecycle === "ready") {
      this.sessionScope = { namespaceId, sessionId };
    }
    return result;
  }

  private async queueExchange(
    namespaceId: bigint,
    sessionId: bigint,
    correlationId: bigint,
    ticketId: bigint,
    requestKind: MessageKind,
    encoder: (
      scope: { namespaceId: bigint; sessionId: bigint; correlationId: bigint },
      ticketId: bigint,
    ) => Uint8Array,
    signal: AbortSignal | undefined,
    runnerOwned: boolean,
  ): Promise<QueueUpdate> {
    const frame = encoder({ namespaceId, sessionId, correlationId }, ticketId);
    const reply = await this.admissionExchange(
      frame,
      namespaceId,
      sessionId,
      correlationId,
      requestKind,
      MessageKind.QueueUpdate,
      signal,
      runnerOwned,
    );
    if (!(reply.control instanceof QueueUpdatePayload)) {
      this.shutdown("admission protocol mismatch");
      throw err("protocol", "queue reply payload mismatch");
    }
    const update = normalizeQueueUpdate(reply.control);
    if (update.ticketId !== ticketId) {
      this.shutdown("admission protocol mismatch");
      throw err("protocol", "queue reply ticket mismatch");
    }
    if (update.state === QueueState.Admitted && this.lifecycle === "ready") {
      this.sessionScope = { namespaceId, sessionId };
    }
    return update;
  }

  private async admissionExchange(
    frame: Uint8Array,
    namespaceId: bigint,
    sessionId: bigint,
    correlationId: bigint,
    requestKind: MessageKind,
    expectedKind: MessageKind,
    signal: AbortSignal | undefined,
    runnerOwned: boolean,
  ): Promise<DecodedEnvelope> {
    this.assertReady();
    if (
      (!runnerOwned && this.admissionRunnerActive) ||
      this.admissionExchangeActive ||
      this.readActive ||
      this.writeActive ||
      this.deferredReceive !== null ||
      this.hasDatagramOperation()
    ) {
      throw err("transport", "client already has an active stream or admission operation");
    }
    this.admissionExchangeActive = true;
    try {
      if (signal?.aborted) throw admissionAbortError(signal);
      const reply = await withAdmissionBounds(
        (async () => {
          await this.writeRaw(frame);
          return this.receiveExclusive((envelope) =>
            isAdmissionCandidate(envelope, namespaceId, sessionId, correlationId),
          );
        })(),
        signal,
      );
      if (isProtocolError(reply)) {
        const errorPayload = currentProtocolError(
          reply,
          requestKind,
          namespaceId,
          sessionId,
          correlationId,
        );
        if (errorPayload === null) {
          throw err("protocol", "ProtocolError does not match the active admission operation");
        }
        throw err("server", errorPayload.message() ?? "server rejected managed admission operation");
      }
      if (reply.messageKind !== expectedKind) {
        throw err("protocol", "unexpected admission reply kind for current correlation");
      }
      return reply;
    } catch (error) {
      this.shutdown("admission stopped");
      throw error;
    } finally {
      this.admissionExchangeActive = false;
    }
  }

  private async waitAdmission(
    namespaceId: bigint,
    sessionId: bigint,
    idempotencyKey: string,
    signal: AbortSignal,
  ): Promise<ManagedAdmissionOutcome> {
    const first = await this.requestAdmissionInternal(
      namespaceId,
      sessionId,
      1n,
      idempotencyKey,
      signal,
      true,
    );
    if (first.status !== AdmissionStatus.Queued) {
      return { kind: "admission", result: first };
    }

    const ticketId = first.ticketId!;
    let correlationId = 2n;
    let pollAfterMs = first.pollAfterMs;
    let offered = false;
    for (;;) {
      await sleepWithAbort(clampPollDelay(pollAfterMs), signal);
      const update: QueueUpdate = offered
        ? await this.queueExchange(
            namespaceId,
            sessionId,
            correlationId,
            ticketId,
            MessageKind.QueueClaim,
            encodeQueueClaim,
            signal,
            true,
          )
        : await this.queueExchange(
            namespaceId,
            sessionId,
            correlationId,
            ticketId,
            MessageKind.QueueHeartbeat,
            encodeQueueHeartbeat,
            signal,
            true,
          );
      if (update.state !== QueueState.Waiting && update.state !== QueueState.Offered) {
        return { kind: "queue", update };
      }
      correlationId += 1n;
      offered = update.state === QueueState.Offered;
      pollAfterMs = update.pollAfterMs;
    }
  }

  private takeOrBeginReceive(): Promise<DecodedEnvelope> {
    if (this.deferredReceive !== null) {
      const receive = this.deferredReceive;
      this.deferredReceive = null;
      return receive;
    }
    return this.receiveExclusive();
  }

  private async receiveExclusive(
    matcher: (envelope: DecodedEnvelope) => boolean = () => true,
  ): Promise<DecodedEnvelope> {
    if (this.readActive) {
      throw err("transport", "another control-stream receive is already active");
    }
    this.readActive = true;
    try {
      for (;;) {
        const pendingIndex = this.pending.findIndex(matcher);
        if (pendingIndex >= 0) {
          return this.pending.splice(pendingIndex, 1)[0]!;
        }
        if (this.lifecycle === "closed") {
          throw err("closed", "connection closed");
        }
        this.ingestChunk(await this.readChunk());
      }
    } catch (error) {
      if (error instanceof CodecError) {
        this.shutdown("protocol violation");
        throw err("protocol", error.message);
      }
      if (isWovenError(error)) throw error;
      this.shutdown("control stream failed");
      throw err(
        "transport",
        error instanceof Error ? error.message : "control stream receive failed",
      );
    } finally {
      this.readActive = false;
    }
  }

  private ingestChunk(chunk: Uint8Array): void {
    let offset = 0;
    while (offset < chunk.length) {
      if (this.inBuffer.length < 4) {
        const prefixBytes = Math.min(4 - this.inBuffer.length, chunk.length - offset);
        this.appendIncoming(chunk.subarray(offset, offset + prefixBytes));
        offset += prefixBytes;
        if (this.inBuffer.length < 4) return;
      }

      const frameLength = this.codec.expectedFrameLength(this.inBuffer);
      if (frameLength === null) return;
      const frameBytes = Math.min(frameLength - this.inBuffer.length, chunk.length - offset);
      this.appendIncoming(chunk.subarray(offset, offset + frameBytes));
      offset += frameBytes;
      if (this.inBuffer.length < frameLength) return;

      if (this.pending.length >= MAX_PENDING_ENVELOPES) {
        throw new CodecError(
          "PendingQueueFull",
          `pending envelope queue exceeds limit ${MAX_PENDING_ENVELOPES}`,
        );
      }
      const envelope = this.codec.decode(this.inBuffer);
      this.observeSessionScope(envelope);
      this.pending.push(envelope);
      this.inBuffer = new Uint8Array(0);
    }
  }

  private observeSessionScope(envelope: DecodedEnvelope): void {
    const scope = this.sessionScope;
    if (scope === null) return;
    const matching = envelope.namespaceId === scope.namespaceId && envelope.sessionId === scope.sessionId;
    if (envelope.messageKind === MessageKind.LeaveSession && matching) this.sessionScope = null;
    if (!(envelope.control instanceof ProtocolErrorPayload)) return;
    const error = envelope.control;
    const unscoped = envelope.namespaceId === 0n && envelope.sessionId === 0n;
    if ((matching || unscoped) &&
        (error.relatedMessageKind() === MessageKind.JoinSession ||
         (error.relatedMessageKind() === MessageKind.ClientLog &&
          [ProtocolErrorCode.AuthenticationRequired, ProtocolErrorCode.Unauthorized, ProtocolErrorCode.InvalidScope].includes(error.code())))) {
      this.sessionScope = null;
    }
  }

  private appendIncoming(bytes: Uint8Array): void {
    const accumulated = this.inBuffer.length + bytes.length;
    if (accumulated > this.codec.maxFrameBytes) {
      throw new CodecError(
        "FrameTooLarge",
        `accumulated frame length ${accumulated} exceeds limit ${this.codec.maxFrameBytes}`,
      );
    }
    this.inBuffer = concat(this.inBuffer, bytes);
  }

  private async readChunk(): Promise<Uint8Array> {
    const reader = this.stream.readable.getReader();
    try {
      const { value, done } = await reader.read();
      if (done) {
        this.lifecycle = "closed";
        this.sessionScope = null;
        throw err("closed", "control stream ended");
      }
      return value;
    } finally {
      reader.releaseLock();
    }
  }

  private requirePositionedState(): void {
    if (!this.supportsPositionedState()) {
      throw err("protocol", "server did not negotiate positioned EntityState");
    }
  }

  private configureDatagramLane(config: WovenConfig): void {
    const lane = this.transport.datagrams;
    if (!this.isDatagramLaneAvailable(lane)) {
      if (config.webTransportOptions?.requireUnreliable) {
        throw err("transport", "unreliable WebTransport datagrams are not available");
      }
      return;
    }
    try {
      lane.incomingHighWaterMark = DATAGRAM_INCOMING_HIGH_WATER_MARK;
      lane.outgoingHighWaterMark = DATAGRAM_OUTGOING_HIGH_WATER_MARK;
      lane.incomingMaxAge = config.datagramMaxAgeMs ?? null;
      lane.outgoingMaxAge = config.datagramMaxAgeMs ?? null;
      if (
        lane.incomingHighWaterMark !== DATAGRAM_INCOMING_HIGH_WATER_MARK ||
        lane.outgoingHighWaterMark !== DATAGRAM_OUTGOING_HIGH_WATER_MARK
      ) {
        throw new Error("runtime did not apply datagram queue bounds");
      }
      this.datagramLane = lane;
    } catch (error) {
      throw err(
        "transport",
        `cannot configure bounded WebTransport datagrams: ${runtimeErrorMessage(error, "queue setters unavailable")}`,
      );
    }
  }

  private isDatagramLaneAvailable(
    lane: WebTransportDatagramDuplexStream | undefined,
  ): lane is WebTransportDatagramDuplexStream {
    return this.transport.reliability !== "reliable-only" &&
      this.transport.reliability !== "pending" && lane !== undefined && lane !== null &&
      Number.isSafeInteger(lane.maxDatagramSize) && lane.maxDatagramSize > 0 &&
      typeof lane.readable?.getReader === "function" && typeof lane.writable?.getWriter === "function";
  }

  private assertDatagramLane(): WebTransportDatagramDuplexStream {
    this.assertReady();
    if (this.admissionRunnerActive || this.admissionExchangeActive) {
      throw err("transport", "datagram lane is unavailable during managed admission");
    }
    if (this.datagramLane === null || !this.isDatagramLaneAvailable(this.datagramLane)) {
      throw err("transport", "unreliable WebTransport datagrams are not available");
    }
    return this.datagramLane;
  }

  private hasDatagramOperation(): boolean {
    return this.datagramReceiveActive || this.datagramReadActive || this.datagramWriteActive ||
      this.pendingDatagramReceive !== null;
  }

  private beginDatagramReceive(): void {
    if (this.datagramReceiveActive) {
      throw err("transport", "another datagram receive is already active");
    }
    this.datagramReceiveActive = true;
  }

  private takeOrBeginDatagramReceive(
    lane: WebTransportDatagramDuplexStream,
  ): PendingDatagramReceive {
    if (this.pendingDatagramReceive !== null) return this.pendingDatagramReceive;
    const receive: PendingDatagramReceive = {
      promise: this.receiveDatagramExclusive(lane), outcome: null, notify: null,
    };
    this.pendingDatagramReceive = receive;
    // Observe each read only once. Repeated timeouts replace the waiter instead of
    // accumulating Promise.race handlers on a read that may never receive a packet.
    const settle = (outcome: DatagramOutcome): void => {
      receive.outcome = outcome;
      receive.notify?.();
    };
    void receive.promise.then(
      (envelope) => settle({ ok: true, envelope }),
      (error) => settle({ ok: false, error }),
    );
    return receive;
  }

  private waitForDatagramReceive(
    receive: PendingDatagramReceive,
    timeoutMs?: number,
  ): Promise<DecodedEnvelope | null> {
    return new Promise((resolve, reject) => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const finish = (): void => {
        const outcome = receive.outcome;
        if (outcome === null) return;
        if (timer !== undefined) clearTimeout(timer);
        receive.notify = null;
        if (this.pendingDatagramReceive === receive) this.pendingDatagramReceive = null;
        if (outcome.ok) resolve(outcome.envelope);
        else reject(outcome.error);
      };
      receive.notify = finish;
      if (receive.outcome !== null) finish();
      else if (timeoutMs !== undefined) {
        timer = setTimeout(() => {
          receive.notify = null;
          resolve(null);
        }, timeoutMs);
      }
    });
  }

  private async receiveDatagramExclusive(
    lane: WebTransportDatagramDuplexStream,
  ): Promise<DecodedEnvelope | null> {
    if (this.datagramEnded) return null;
    if (this.datagramReadActive) throw err("transport", "another datagram read is already active");
    this.datagramReadActive = true;
    let reader: ReadableStreamDefaultReader<Uint8Array> | null = null;
    try {
      reader = lane.readable.getReader();
      this.datagramReader = reader;
      const stopped = new Promise<ReadableStreamReadResult<Uint8Array>>((resolve) => {
        this.stopDatagramRead = () => resolve({ done: true, value: undefined });
      });
      const { value, done } = await Promise.race([reader.read(), stopped]);
      if (done || this.datagramEnded) {
        this.datagramEnded = true;
        return null;
      }
      const envelope = this.codec.decode(value);
      validateUnreliableEntityState(envelope);
      return envelope;
    } catch (error) {
      if (this.datagramEnded) return null;
      if (error instanceof CodecError) {
        throw err("protocol", `invalid datagram (packet discarded): ${error.message}`);
      }
      if (isWovenError(error)) throw error;
      throw err("transport", `datagram receive failed: ${runtimeErrorMessage(error, "unknown transport error")}`);
    } finally {
      this.stopDatagramRead = null;
      this.datagramReader = null;
      reader?.releaseLock();
      this.datagramReadActive = false;
    }
  }

  private async writeDatagram(
    frame: Uint8Array,
    lane: WebTransportDatagramDuplexStream,
  ): Promise<void> {
    if (this.datagramEnded) throw err("closed", "datagram lane ended");
    this.datagramWriteActive = true;
    let writer: WritableStreamDefaultWriter<Uint8Array> | null = null;
    try {
      writer = lane.writable.getWriter();
      this.datagramWriter = writer;
      const stopped = new Promise<never>((_resolve, reject) => {
        this.stopDatagramWrite = () => reject(err("closed", "datagram lane ended"));
      });
      await Promise.race([writer.write(frame), stopped]);
    } catch (error) {
      if (isWovenError(error)) throw error;
      throw err("transport", `datagram write failed: ${runtimeErrorMessage(error, "unknown transport error")}`);
    } finally {
      this.stopDatagramWrite = null;
      this.datagramWriter = null;
      writer?.releaseLock();
      this.datagramWriteActive = false;
    }
  }

  private stopDatagramLane(): void {
    this.datagramEnded = true;
    this.pendingDatagramReceive = null;
    this.stopDatagramRead?.();
    this.stopDatagramWrite?.();
    // Do not await runtime cancellation: a broken shim must not make client shutdown unbounded.
    void this.datagramReader?.cancel("connection closed").catch(() => {});
    void this.datagramWriter?.abort("connection closed").catch(() => {});
  }

  private validatePublishPayload(payload: Uint8Array): void {
    this.assertReady();
    try {
      validatePayloadLength(payload.byteLength, this.maxPublishPayloadBytes);
    } catch (error) {
      if (error instanceof CodecError) throw err("protocol", error.message);
      throw error;
    }
  }

  private async write(frame: Uint8Array): Promise<void> {
    this.assertReady();
    if (this.admissionRunnerActive || this.admissionExchangeActive) {
      throw err("transport", "admission operation owns the control stream");
    }
    await this.writeRaw(frame);
  }

  private async writeRaw(frame: Uint8Array): Promise<void> {
    if (this.lifecycle === "closed") {
      throw err("closed", "connection closed");
    }
    if (frame.byteLength > this.maxPublishFrameBytes) {
      throw err(
        "protocol",
        `encoded frame length ${frame.byteLength} bytes exceeds frame limit ${this.maxPublishFrameBytes} bytes`,
      );
    }
    if (this.writeActive) {
      throw err("transport", "another control-stream write is already active");
    }
    this.writeActive = true;
    try {
      const writer = this.stream.writable.getWriter();
      try {
        await writer.write(frame);
      } finally {
        writer.releaseLock();
      }
    } catch (error) {
      this.sessionScope = null;
      throw error;
    } finally {
      this.writeActive = false;
    }
  }

  private assertReady(): void {
    if (this.lifecycle !== "ready") {
      throw err("closed", "connection is not ready");
    }
  }

  private initiateClose(closeCode: number, reason: string): void {
    this.lifecycle = "closed";
    this.sessionScope = null;
    this.stopDatagramLane();
    if (this.closeInitiated) return;
    this.transport.close({ closeCode, reason });
    this.closeInitiated = true;
  }

  private shutdown(reason: string): void {
    if (this.closeInitiated) return;
    this.initiateClose(0, reason);
  }
}

function normalizeAdmissionResult(payload: AdmissionResultPayload): AdmissionResult {
  return {
    status: payload.status(),
    rejectionCode: payload.rejectionCode(),
    ticketId: payload.ticketId() === 0n ? null : payload.ticketId(),
    pollAfterMs: payload.pollAfterMs(),
    ticketRemainingMs: payload.ticketRemainingMs(),
  };
}

function normalizeQueueUpdate(payload: QueueUpdatePayload): QueueUpdate {
  return {
    ticketId: payload.ticketId(),
    state: payload.state(),
    position: payload.position(),
    pollAfterMs: payload.pollAfterMs(),
    ticketRemainingMs: payload.ticketRemainingMs(),
    offerRemainingMs: payload.offerRemainingMs(),
  };
}

function clampPollDelay(adviceMs: number): number {
  return Math.min(5_000, Math.max(1_000, adviceMs));
}

async function sleepWithAbort(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) throw admissionAbortError(signal);
  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const finish = (action: () => void): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
      action();
    };
    const onAbort = (): void => finish(() => reject(admissionAbortError(signal)));
    const timer = setTimeout(() => finish(resolve), ms);
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) onAbort();
  });
}

async function withAdmissionBounds<T>(
  promise: Promise<T>,
  signal: AbortSignal | undefined,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let onAbort: (() => void) | undefined;
  try {
    const timeout = new Promise<T>((_resolve, reject) => {
      timer = setTimeout(
        () => reject(err("transport", "admission operation timed out; connection closed")),
        ADMISSION_EXCHANGE_TIMEOUT_MS,
      );
    });
    if (signal === undefined) return await Promise.race([promise, timeout]);
    const aborted = new Promise<T>((_resolve, reject) => {
      onAbort = () => reject(admissionAbortError(signal));
      if (signal.aborted) onAbort();
      else signal.addEventListener("abort", onAbort, { once: true });
    });
    return await Promise.race([promise, timeout, aborted]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    if (signal !== undefined && onAbort !== undefined) {
      signal.removeEventListener("abort", onAbort);
    }
  }
}

function admissionAbortError(signal: AbortSignal): WovenError {
  return signal.reason === "deadline"
    ? err("transport", "admission deadline exceeded; connection closed")
    : err("transport", "admission cancelled; connection closed");
}

const MAX_TIMER_TIMEOUT_MS = 2_147_483_647;

function validatedClientLimits(config: WovenConfig): ClientLimits {
  const maxFrameBytes = config.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES;
  const maxPayloadBytes = config.maxPayloadBytes ?? DEFAULT_MAX_PAYLOAD_BYTES;
  if (
    !Number.isInteger(maxFrameBytes) ||
    !Number.isInteger(maxPayloadBytes) ||
    maxFrameBytes < 12 ||
    maxFrameBytes > 0xffff_ffff ||
    maxPayloadBytes <= 0 ||
    maxPayloadBytes > maxFrameBytes
  ) {
    throw new Error(
      "maxFrameBytes and maxPayloadBytes must be positive uint32 integers, with frame >= 12 and payload <= frame",
    );
  }
  return { maxFrameBytes, maxPayloadBytes };
}

function validateDatagramMaxAge(value: number | undefined): void {
  if (value !== undefined && (!Number.isFinite(value) || value <= 0 || value > MAX_TIMER_TIMEOUT_MS)) {
    throw err(
      "transport",
      `datagramMaxAgeMs must be positive, finite, and no greater than ${MAX_TIMER_TIMEOUT_MS}`,
    );
  }
}

function validatedConnectionTimeout(value: number | undefined): number {
  const timeout = value ?? 10_000;
  if (!Number.isFinite(timeout) || timeout <= 0 || timeout > MAX_TIMER_TIMEOUT_MS) {
    throw new Error(
      `connectTimeoutMs must be a positive finite number no greater than ${MAX_TIMER_TIMEOUT_MS}`,
    );
  }
  return timeout;
}

function validatedGracefulCloseTimeout(value: number): number {
  if (!Number.isFinite(value) || value <= 0 || value > MAX_GRACEFUL_CLOSE_TIMEOUT_MS) {
    throw new Error(
      `timeoutMs must be a positive finite number no greater than ${MAX_GRACEFUL_CLOSE_TIMEOUT_MS}`,
    );
  }
  return value;
}

function connectionNow(): number {
  return performance.now();
}

function remainingConnectionTime(deadline: number, timeoutMessage: string): number {
  const remaining = deadline - connectionNow();
  if (remaining <= 0) throw timeoutError(timeoutMessage);
  return remaining;
}

function timeoutError(message: string): Error & { name: string } {
  const error = new Error(message) as Error & { name: string };
  error.name = "TimeoutError";
  return error;
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length);
  out.set(a);
  out.set(b, a.length);
  return out;
}

function isHandshakeEnvelope(envelope: DecodedEnvelope): boolean {
  return (
    envelope.protocolVersion === PROTOCOL_VERSION &&
    envelope.deliveryClass === DeliveryClass.ReliableOrdered &&
    envelope.namespaceId === 0n &&
    envelope.sessionId === 0n &&
    envelope.spaceId === 0n &&
    envelope.spaceEpoch === 0n &&
    envelope.channelId === null &&
    envelope.entityId === null &&
    envelope.correlationId === null &&
    envelope.payloadTypeId === 0n &&
    (envelope.payload === null || envelope.payload.length === 0)
  );
}

function isAdmissionCandidate(
  envelope: DecodedEnvelope,
  namespaceId: bigint,
  sessionId: bigint,
  correlationId: bigint,
): boolean {
  return (
    envelope.namespaceId === namespaceId &&
    envelope.sessionId === sessionId &&
    envelope.correlationId === correlationId
  );
}

function currentProtocolError(
  envelope: DecodedEnvelope,
  relatedKind: MessageKind,
  namespaceId: bigint,
  sessionId: bigint,
  correlationId: bigint | null,
): ProtocolErrorPayload | null {
  if (
    envelope.messageKind !== MessageKind.ProtocolError ||
    envelope.controlType !== ControlPayload.ProtocolErrorPayload ||
    !(envelope.control instanceof ProtocolErrorPayload) ||
    envelope.namespaceId !== namespaceId ||
    envelope.sessionId !== sessionId ||
    envelope.spaceId !== 0n ||
    envelope.spaceEpoch !== 0n ||
    envelope.channelId !== null ||
    envelope.entityId !== null ||
    envelope.correlationId !== correlationId ||
    envelope.control.relatedMessageKind() !== relatedKind
  ) {
    return null;
  }
  return envelope.control;
}

function isWovenError(error: unknown): error is WovenError {
  if (typeof error !== "object" || error === null) return false;
  const value = error as Partial<WovenError>;
  return (
    typeof value.message === "string" &&
    (value.kind === "transport" ||
      value.kind === "protocol" ||
      value.kind === "server" ||
      value.kind === "handshake" ||
      value.kind === "closed")
  );
}

function runtimeErrorMessage(error: unknown, fallback: string): string {
  if (!(error instanceof Error)) return fallback;
  const message = error.message.replace(/[\r\n\t]+/g, " ").trim();
  if (message.length === 0) return fallback;
  return message.slice(0, 256);
}

function kindName(kind: MessageKind): string {
  return MessageKind[kind] ?? `MessageKind(${kind})`;
}

async function withTimeout<T>(
  promise: Promise<T>,
  ms: number,
  message: string | undefined,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        timer = setTimeout(() => reject(timeoutError(message ?? "timed out")), ms);
      }),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}
