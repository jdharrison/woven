import { EnvelopeCodec, DecodedEnvelope } from "./codec.js";
import {
  encodeHello,
  encodeAuthenticate,
  encodeJoinSession,
  encodeSubscribeSpace,
  encodeSnapshotRequest,
  encodeSpaceTransition,
  encodeInferenceRequested,
  encodeReliableEvent,
  encodeEntityState,
} from "./encode.js";
import {
  WebTransport,
  WebTransportBidirectionalStream,
  resolveWebTransportConstructor,
} from "./webtransport.js";
import { toWebTransportUrl } from "./url.js";
import { MessageKind, ControlPayload } from "../generated/woven/protocol/v1.js";

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
  /** Bearer token sent during the `Authenticate` handshake step. */
  token: string;
  /** Maximum frame size advertised in `Hello` (bytes). */
  maxFrameBytes?: number;
  /** Maximum payload size advertised in `Hello` (bytes). */
  maxPayloadBytes?: number;
  /** Milliseconds to wait for the WebTransport session to become ready. */
  connectTimeoutMs?: number;
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
  private readonly codec = new EnvelopeCodec();
  private inBuffer: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  private pending: DecodedEnvelope[] = [];
  private closed = false;

  private constructor(
    transport: WebTransport,
    stream: WebTransportBidirectionalStream,
  ) {
    this.transport = transport;
    this.stream = stream;
  }

  /**
   * Connect to a Woven server over WebTransport and complete the protocol
   * handshake.
   */
  static async connect(
    config: WovenConfig,
  ): Promise<WovenClient> {
    const WebTransportCtor = resolveWebTransportConstructor();
    const url = toWebTransportUrl(config.url);
    const connectTimeoutMs = config.connectTimeoutMs ?? 10_000;

    const transport = new WebTransportCtor(url);
    await withTimeout(transport.ready, connectTimeoutMs, "WebTransport connect timed out")
      .catch((e) => {
        transport.close();
        throw e;
      });
    const stream = await transport.createBidirectionalStream();
    return WovenClient.fromTransport(transport, stream, config);
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
    const client = new WovenClient(transport, stream);
    await client.handshake(config);
    return client;
  }

  /** Send a `JoinSession` control envelope. */
  async joinSession(namespaceId: bigint, sessionId: bigint): Promise<void> {
    await this.write(encodeJoinSession({ namespaceId, sessionId }));
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
   * Send a reliable event opaque payload envelope.
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
   * Send a latest-value (entity state) opaque payload envelope.
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
    await this.write(
      encodeInferenceRequested(
        { namespaceId, sessionId, spaceId, spaceEpoch, entityId: aiEntityId },
        { capability, deadlineMs, input },
      ),
    );
  }

  /**
   * Receive the next decoded envelope from the control stream, blocking until
   * one arrives.
   */
  async recv(): Promise<DecodedEnvelope> {
    for (;;) {
      if (this.pending.length > 0) {
        return this.pending.shift()!;
      }
      if (this.closed) {
        throw err("closed", "connection closed");
      }
      const chunk = await this.readChunk();
      this.inBuffer = concat(this.inBuffer, chunk);
      this.drainBuffer();
      if (this.pending.length === 0 && this.inBuffer.length === 0 && !this.closed) {
        throw err("closed", "connection closed by server");
      }
    }
  }

  /** Try to receive the next envelope within `timeoutMs`. Returns `null` on timeout. */
  async recvTimeout(timeoutMs: number): Promise<DecodedEnvelope | null> {
    return withTimeout(this.recv(), timeoutMs, undefined).then(
      (e) => e,
      (e) => {
        if (e && (e as { name?: string }).name === "TimeoutError") return null;
        throw e;
      },
    );
  }

  /** Close the WebTransport session gracefully. */
  close(closeCode = 0, reason = "client closed"): void {
    this.closed = true;
    this.transport.close({ closeCode, reason });
  }

  private async handshake(config: WovenConfig): Promise<void> {
    await this.write(
      encodeHello({
        clientName: "woven-client-ts",
        clientVersion: "0.1.0",
        maxFrameSize: config.maxFrameBytes ?? 65536,
        maxPayloadSize: config.maxPayloadBytes ?? 65536,
      }),
    );
    const capabilities = await this.expectKind(MessageKind.Capabilities);
    if (capabilities.controlType === ControlPayload.ProtocolErrorPayload) {
      throw err("server", "Capabilities stage rejected by server");
    }

    await this.write(encodeAuthenticate(new TextEncoder().encode(config.token)));
    const authenticated = await this.expectKind(MessageKind.Authenticated);
    if (authenticated.controlType === ControlPayload.ProtocolErrorPayload) {
      throw err("server", "authentication rejected by server");
    }
  }

  private async expectKind(kind: MessageKind): Promise<DecodedEnvelope> {
    for (;;) {
      const envelope = await this.recv();
      if (isProtocolError(envelope)) {
        throw err("server", `ProtocolError: ${kindName(envelope.messageKind)}`);
      }
      if (envelope.messageKind === kind) {
        return envelope;
      }
      throw err("handshake", `expected ${kindName(kind)}, got ${kindName(envelope.messageKind)}`);
    }
  }

  private drainBuffer(): void {
    for (;;) {
      const result = this.codec.decodeStream(this.inBuffer);
      if (result === null) break;
      this.pending.push(result.envelope);
      this.inBuffer = this.inBuffer.subarray(result.consumed);
    }
  }

  private async readChunk(): Promise<Uint8Array> {
    const reader = this.stream.readable.getReader();
    const { value, done } = await reader.read();
    reader.releaseLock();
    if (done) {
      this.closed = true;
      throw err("closed", "control stream ended");
    }
    return value;
  }

  private async write(frame: Uint8Array): Promise<void> {
    if (this.closed) {
      throw err("closed", "connection closed");
    }
    const writer = this.stream.writable.getWriter();
    try {
      await writer.write(frame);
    } finally {
      writer.releaseLock();
    }
  }
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const out = new Uint8Array(a.length + b.length);
  out.set(a);
  out.set(b);
  return out;
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
        timer = setTimeout(() => {
          const e: Error & { name?: string } = new Error(message ?? "timed out");
          e.name = "TimeoutError";
          reject(e);
        }, ms);
      }),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}
