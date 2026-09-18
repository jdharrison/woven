import { test, describe, beforeEach } from "node:test";
import { strict as assert } from "node:assert";
import { WovenClient } from "../src/client.js";
import { EnvelopeCodec, DecodedEnvelope } from "../src/codec.js";
import { MessageKind, ControlPayload } from "../generated/woven/protocol/v1.js";
import {
  WebTransport,
  WebTransportBidirectionalStream,
  WebTransportOptions,
} from "../src/webtransport.js";
import {
  encodeHello,
  encodeAuthenticate,
  encodeJoinSession,
  encodeSubscribeSpace,
  encodeReliableEvent,
} from "../src/encode.js";
import { buildAuthenticated, buildCapabilities } from "./wire-helpers.js";

const codec = new EnvelopeCodec();
const encoder = new TextEncoder();


/**
 * A minimal in-memory WebTransport server that speaks enough of the Woven
 * protocol to handshake: on Hello it queues a Capabilities frame, on Authenticate
 * it queues an Authenticated frame, and it records every decoded request.
 */
class FakeServer {
  readonly requests: DecodedEnvelope[] = [];
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  readonly bidi: WebTransportBidirectionalStream;
  private controller: ReadableStreamDefaultController<Uint8Array> | null = null;
  private pushQueue: Uint8Array[] = [];
  private acc = new Uint8Array(0);

  constructor(
    private readonly respondToHello = true,
    private readonly respondToAuthenticate = true,
    private readonly capabilitiesFrame = buildCapabilities(),
    private readonly authenticatedFrame = buildAuthenticated(),
  ) {
    const self = this;
    this.readable = new ReadableStream<Uint8Array>({
      start: (c) => {
        this.controller = c;
      },
      pull: () => {
        while (this.pushQueue.length > 0) {
          this.controller?.enqueue(this.pushQueue.shift()!);
        }
      },
    });
    this.writable = new WritableStream({
      write(chunk: Uint8Array) {
        self.ingest(new Uint8Array(chunk));
      },
    });
    this.bidi = { readable: this.readable, writable: this.writable };
  }

  pushFrame(frame: Uint8Array): void {
    if (this.controller) this.controller.enqueue(frame);
    else this.pushQueue.push(frame);
  }

  private ingest(chunk: Uint8Array): void {
    const merged = new Uint8Array(this.acc.length + chunk.length);
    merged.set(this.acc);
    merged.set(chunk, this.acc.length);
    this.acc = merged;
    for (;;) {
      const result = codec.decodeStream(this.acc);
      if (result === null) break;
      this.acc = this.acc.subarray(result.consumed);
      const envelope = result.envelope;
      this.requests.push(envelope);
      if (envelope.messageKind === MessageKind.Hello && this.respondToHello) {
        this.pushFrame(this.capabilitiesFrame);
      } else if (
        envelope.messageKind === MessageKind.Authenticate &&
        this.respondToAuthenticate
      ) {
        this.pushFrame(this.authenticatedFrame);
      }
    }
  }
}

function makeWebTransport(server: FakeServer, onClose: () => void = () => {}): WebTransport {
  return {
    ready: Promise.resolve(),
    closed: Promise.resolve({ closeCode: 0 }),
    datagrams: {
      readable: new ReadableStream(),
      writable: new WritableStream(),
      incomingMaxAge: null,
      outgoingMaxAge: null,
      incomingHighWaterMark: 0,
      outgoingHighWaterMark: 0,
    },
    createBidirectionalStream: async () => server.bidi,
    close: onClose,
  } as WebTransport;
}

describe("WovenClient handshake over mocked WebTransport", () => {
  let server: FakeServer;
  let wt: WebTransport;

  beforeEach(() => {
    server = new FakeServer();
    wt = makeWebTransport(server);
  });

  test("completes Hello -> Capabilities -> Authenticate -> Authenticated", async () => {
    const client = await WovenClient.fromTransport(wt, server.bidi, {
      url: "https://localhost:4433/webtransport",
      token: "dev-token",
      connectTimeoutMs: 1_000,
    });
    assert.equal(server.requests[0]!.messageKind, MessageKind.Hello);
    assert.equal(server.requests[1]!.messageKind, MessageKind.Authenticate);
    client.close();
  });

  test("rejects semantically invalid Capabilities and Authenticated payloads", async () => {
    const cases = [
      new FakeServer(true, true, buildCapabilities({ selectedProtocolVersion: 2 })),
      new FakeServer(true, true, buildCapabilities({ maxFrameSize: 0 })),
      new FakeServer(true, true, buildCapabilities({ maxFrameSize: 64, maxPayloadSize: 65 })),
      new FakeServer(true, true, buildCapabilities({ envelopeProtocolVersion: 2 })),
      new FakeServer(
        true,
        true,
        buildCapabilities({ controlType: ControlPayload.AuthenticatedPayload }),
      ),
      new FakeServer(true, true, buildCapabilities(), buildAuthenticated({ principalId: 0n })),
      new FakeServer(
        true,
        true,
        buildCapabilities(),
        buildAuthenticated({ controlType: ControlPayload.CapabilitiesPayload }),
      ),
    ];

    for (const invalidServer of cases) {
      let closeCount = 0;
      await assert.rejects(
        WovenClient.fromTransport(
          makeWebTransport(invalidServer, () => {
            closeCount += 1;
          }),
          invalidServer.bidi,
          {
            url: "https://localhost:4433/webtransport",
            token: "dev-token",
            connectTimeoutMs: 1_000,
          },
        ),
      );
      assert.equal(closeCount, 1);
    }
  });

  test("rejects invalid advertised client limits before handshake I/O", async () => {
    const untouchedServer = new FakeServer();
    const untouchedTransport = makeWebTransport(untouchedServer);
    for (const limits of [
      { maxFrameBytes: 0, maxPayloadBytes: 1 },
      { maxFrameBytes: 64, maxPayloadBytes: 0 },
      { maxFrameBytes: 64, maxPayloadBytes: 65 },
      { maxFrameBytes: 64.5, maxPayloadBytes: 32 },
      { maxFrameBytes: 0x1_0000_0000, maxPayloadBytes: 32 },
    ]) {
      await assert.rejects(
        WovenClient.fromTransport(untouchedTransport, untouchedServer.bidi, {
          url: "https://localhost:4433/webtransport",
          token: "dev-token",
          ...limits,
        }),
        /maxFrameBytes and maxPayloadBytes/,
      );
    }
    assert.equal(untouchedServer.requests.length, 0);
  });

  test("a stalled WVN1 handshake times out and closes an injected transport", async () => {
    const stalledServer = new FakeServer(false);
    let closeCount = 0;
    const stalledTransport = makeWebTransport(stalledServer, () => {
      closeCount += 1;
    });

    await assert.rejects(
      WovenClient.fromTransport(stalledTransport, stalledServer.bidi, {
        url: "https://localhost:4433/webtransport",
        token: "dev-token",
        connectTimeoutMs: 20,
      }),
      (error: unknown) => {
        const value = error as Error;
        return value.name === "TimeoutError" && value.message === "WVN1 handshake timed out";
      },
    );
    assert.equal(stalledServer.requests[0]?.messageKind, MessageKind.Hello);
    assert.equal(closeCount, 1);
  });

  test("fromTransport rejects invalid timeouts before handshake I/O", async () => {
    const untouchedServer = new FakeServer();
    let closeCount = 0;
    const untouchedTransport = makeWebTransport(untouchedServer, () => {
      closeCount += 1;
    });

    for (const connectTimeoutMs of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, 2_147_483_648]) {
      await assert.rejects(
        WovenClient.fromTransport(untouchedTransport, untouchedServer.bidi, {
          url: "https://localhost:4433/webtransport",
          token: "dev-token",
          connectTimeoutMs,
        }),
        /connectTimeoutMs must be a positive finite number/,
      );
    }
    assert.equal(untouchedServer.requests.length, 0);
    assert.equal(closeCount, 0);
  });

  test("joinSession and subscribeSpace are sent and decoded", async () => {
    const client = await WovenClient.fromTransport(wt, server.bidi, {
      url: "https://localhost:4433/webtransport",
      token: "dev-token",
    });
    await client.joinSession(1n, 1n);
    await client.subscribeSpace(1n, 1n, 1n, 1n, 1n);
    assert.equal(server.requests[2]!.messageKind, MessageKind.JoinSession);
    assert.equal(server.requests[2]!.namespaceId, 1n);
    assert.equal(server.requests[3]!.messageKind, MessageKind.SubscribeSpace);
    assert.equal(server.requests[3]!.spaceId, 1n);
    assert.equal(server.requests[3]!.channelId, 1n);
    client.close();
  });

  test("publishEvent carries the payload and sequence", async () => {
    const client = await WovenClient.fromTransport(wt, server.bidi, {
      url: "https://localhost:4433/webtransport",
      token: "dev-token",
    });
    await client.publishEvent(1n, 1n, 1n, 1n, 1n, 1n, 1n, 1n, encoder.encode("hi"));
    assert.equal(server.requests[2]!.messageKind, MessageKind.ReliableEvent);
    assert.equal(server.requests[2]!.entityId, 1n);
    assert.equal(server.requests[2]!.senderSequence, 1n);
    assert.deepEqual(server.requests[2]!.payload, encoder.encode("hi"));
    client.close();
  });

  test("recv yields envelopes queued by the server", async () => {
    const client = await WovenClient.fromTransport(wt, server.bidi, {
      url: "https://localhost:4433/webtransport",
      token: "dev-token",
    });
    server.pushFrame(encodeReliableEvent(
      { namespaceId: 1n, sessionId: 1n, spaceId: 1n, spaceEpoch: 1n, channelId: 1n, entityId: 5n, senderSequence: 1n },
      { typeId: 1n, bytes: encoder.encode("from-server") },
    ));
    const envelope = await client.recv();
    assert.equal(envelope.messageKind, MessageKind.ReliableEvent);
    assert.equal(envelope.entityId, 5n);
    assert.deepEqual(envelope.payload, encoder.encode("from-server"));
    client.close();
  });

  test("an oversized incoming prefix fails closed before frame accumulation", async () => {
    const boundedServer = new FakeServer();
    let closeCount = 0;
    const client = await WovenClient.fromTransport(
      makeWebTransport(boundedServer, () => {
        closeCount += 1;
      }),
      boundedServer.bidi,
      {
        url: "https://localhost:4433/webtransport",
        token: "dev-token",
        maxFrameBytes: 256,
        maxPayloadBytes: 128,
      },
    );
    const prefix = new Uint8Array(4);
    new DataView(prefix.buffer).setUint32(0, 256, true);
    boundedServer.pushFrame(prefix);
    await assert.rejects(client.recv(), (error: unknown) => {
      const value = error as { kind?: string; message?: string };
      return value.kind === "protocol" && value.message?.includes("exceeds limit") === true;
    });
    assert.equal(closeCount, 1);
  });

  test("a burst beyond the bounded decoded inbox fails closed", async () => {
    const boundedServer = new FakeServer();
    let closeCount = 0;
    const client = await WovenClient.fromTransport(
      makeWebTransport(boundedServer, () => {
        closeCount += 1;
      }),
      boundedServer.bidi,
      { url: "https://localhost:4433/webtransport", token: "dev-token" },
    );
    const frame = encodeReliableEvent(
      {
        namespaceId: 1n,
        sessionId: 1n,
        spaceId: 1n,
        spaceEpoch: 1n,
        channelId: 1n,
        entityId: 1n,
        senderSequence: 1n,
      },
      { typeId: 1n, bytes: encoder.encode("x") },
    );
    const burst = new Uint8Array(frame.length * 65);
    for (let index = 0; index < 65; index += 1) burst.set(frame, index * frame.length);
    boundedServer.pushFrame(burst);

    await assert.rejects(client.recv(), (error: unknown) => {
      const value = error as { kind?: string; message?: string };
      return value.kind === "protocol" && value.message?.includes("pending envelope queue") === true;
    });
    assert.equal(closeCount, 1);
  });
});

describe("WovenClient connect: quic:// derives WebTransport endpoint", () => {
  test("connect maps quic://host:PORT to https://host:(PORT+1)/webtransport", async () => {
    const server = new FakeServer();
    const seenUrl: string[] = [];

    const SpyingWebTransport = function (url: string) {
      seenUrl.push(url);
      return makeWebTransport(server);
    } as unknown as typeof globalThis.WebTransport;

    (globalThis as Record<string, unknown>).WebTransport = SpyingWebTransport;
    try {
      const client = await WovenClient.connect({
        url: "quic://127.0.0.1:8081",
        token: "dev-token",
      });
      assert.deepEqual(seenUrl, ["https://127.0.0.1:8082/webtransport"]);
      client.close();
    } finally {
      delete (globalThis as Record<string, unknown>).WebTransport;
    }
  });

  test("connect applies its timeout to a stalled handshake and closes exactly once", async () => {
    const stalledServer = new FakeServer(false);
    let closeCount = 0;
    const SpyingWebTransport = function (_url: string) {
      return makeWebTransport(stalledServer, () => {
        closeCount += 1;
      });
    } as unknown as typeof globalThis.WebTransport;

    (globalThis as Record<string, unknown>).WebTransport = SpyingWebTransport;
    try {
      await assert.rejects(
        WovenClient.connect({
          url: "https://localhost:4433/webtransport",
          token: "dev-token",
          connectTimeoutMs: 20,
        }),
        (error: unknown) => {
          const value = error as Error;
          return value.name === "TimeoutError" && value.message === "WVN1 handshake timed out";
        },
      );
      assert.equal(stalledServer.requests[0]?.messageKind, MessageKind.Hello);
      assert.equal(closeCount, 1);
    } finally {
      delete (globalThis as Record<string, unknown>).WebTransport;
    }
  });

  test("connect carries only the remaining deadline into the WVN1 handshake", async (context) => {
    const stalledServer = new FakeServer(false);
    let resolveReady!: () => void;
    const ready = new Promise<void>((resolve) => {
      resolveReady = resolve;
    });
    let now = 0;
    const originalNow = performance.now;
    Object.defineProperty(performance, "now", {
      configurable: true,
      value: () => now,
    });
    context.mock.timers.enable({ apis: ["setTimeout"] });

    const SpyingWebTransport = function (_url: string) {
      return { ...makeWebTransport(stalledServer), ready };
    } as unknown as typeof globalThis.WebTransport;
    (globalThis as Record<string, unknown>).WebTransport = SpyingWebTransport;

    try {
      const connection = WovenClient.connect({
        url: "https://localhost:4433/webtransport",
        token: "dev-token",
        connectTimeoutMs: 100,
      });
      let settled = false;
      void connection.then(
        () => {
          settled = true;
        },
        () => {
          settled = true;
        },
      );

      now = 60;
      resolveReady();
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(stalledServer.requests[0]?.messageKind, MessageKind.Hello);

      context.mock.timers.tick(39);
      await Promise.resolve();
      assert.equal(settled, false);

      context.mock.timers.tick(1);
      await assert.rejects(connection, /WVN1 handshake timed out/);
    } finally {
      context.mock.timers.reset();
      Object.defineProperty(performance, "now", {
        configurable: true,
        value: originalNow,
      });
      delete (globalThis as Record<string, unknown>).WebTransport;
    }
  });

  test("connect rejects invalid timeouts before resolving or constructing WebTransport", async () => {
    delete (globalThis as Record<string, unknown>).WebTransport;
    await assert.rejects(
      WovenClient.connect({
        url: "https://localhost:4433/webtransport",
        token: "dev-token",
        connectTimeoutMs: 0,
      }),
      /connectTimeoutMs must be a positive finite number/,
    );

    let constructorCalls = 0;
    const SpyingWebTransport = function (_url: string) {
      constructorCalls += 1;
      return makeWebTransport(new FakeServer());
    } as unknown as typeof globalThis.WebTransport;

    (globalThis as Record<string, unknown>).WebTransport = SpyingWebTransport;
    try {
      for (const connectTimeoutMs of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, 2_147_483_648]) {
        await assert.rejects(
          WovenClient.connect({
            url: "https://localhost:4433/webtransport",
            token: "dev-token",
            connectTimeoutMs,
          }),
          /connectTimeoutMs must be a positive finite number/,
        );
      }
      assert.equal(constructorCalls, 0);
    } finally {
      delete (globalThis as Record<string, unknown>).WebTransport;
    }
  });

  test("connect forwards validated WebTransport constructor options", async () => {
    const server = new FakeServer();
    const seenOptions: (WebTransportOptions | undefined)[] = [];
    const hash = new Uint8Array(32).fill(7);

    const SpyingWebTransport = function (_url: string, options?: WebTransportOptions) {
      seenOptions.push(options);
      return makeWebTransport(server);
    } as unknown as typeof globalThis.WebTransport;

    (globalThis as Record<string, unknown>).WebTransport = SpyingWebTransport;
    try {
      const client = await WovenClient.connect({
        url: "https://localhost:4433/webtransport",
        token: "dev-token",
        webTransportOptions: {
          allowPooling: false,
          requireUnreliable: true,
          congestionControl: "throughput",
          serverCertificateHashes: [{ algorithm: "sha-256", value: hash }],
        },
      });
      assert.equal(seenOptions[0]?.allowPooling, false);
      assert.equal(seenOptions[0]?.requireUnreliable, true);
      assert.equal(seenOptions[0]?.congestionControl, "throughput");
      assert.deepEqual(
        seenOptions[0]?.serverCertificateHashes?.[0]?.value,
        hash,
      );
      assert.notEqual(seenOptions[0]?.serverCertificateHashes?.[0]?.value, hash);
      client.close();
    } finally {
      delete (globalThis as Record<string, unknown>).WebTransport;
    }
  });

  test("connect rejects malformed or excessive server certificate hashes before construction", async () => {
    const calls: string[] = [];
    const SpyingWebTransport = function (url: string) {
      calls.push(url);
      return makeWebTransport(new FakeServer());
    } as unknown as typeof globalThis.WebTransport;

    (globalThis as Record<string, unknown>).WebTransport = SpyingWebTransport;
    try {
      await assert.rejects(
        WovenClient.connect({
          url: "https://localhost:4433/webtransport",
          token: "dev-token",
          webTransportOptions: {
            serverCertificateHashes: [
              { algorithm: "sha-256", value: new Uint8Array(31) },
            ],
          },
        }),
        /must be 32 bytes/,
      );
      await assert.rejects(
        WovenClient.connect({
          url: "https://localhost:4433/webtransport",
          token: "dev-token",
          webTransportOptions: {
            serverCertificateHashes: Array.from({ length: 9 }, () => ({
              algorithm: "sha-256" as const,
              value: new Uint8Array(32),
            })),
          },
        }),
        /cannot exceed 8/,
      );
      assert.deepEqual(calls, []);
    } finally {
      delete (globalThis as Record<string, unknown>).WebTransport;
    }
  });

  test("connect maps quic:// default port 4433 to 4434", async () => {
    const server = new FakeServer();
    const seenUrl: string[] = [];

    const SpyingWebTransport = function (url: string) {
      seenUrl.push(url);
      return makeWebTransport(server);
    } as unknown as typeof globalThis.WebTransport;

    (globalThis as Record<string, unknown>).WebTransport = SpyingWebTransport;
    try {
      const client = await WovenClient.connect({
        url: "quic://relay.example",
        token: "dev-token",
      });
      assert.deepEqual(seenUrl, ["https://relay.example:4434/webtransport"]);
      client.close();
    } finally {
      delete (globalThis as Record<string, unknown>).WebTransport;
    }
  });
});

describe("encode path direct equality with generated bindings", () => {
  test("hello round-trips through the generated Decoder", () => {
    const frame = encodeHello({ clientName: "x", clientVersion: "1" });
    const envelope = codec.decode(frame);
    assert.equal(envelope.messageKind, MessageKind.Hello);
    assert.equal(envelope.controlType, ControlPayload.HelloPayload);
  });

  test("authenticate round-trips through the generated Decoder", () => {
    const frame = encodeAuthenticate(encoder.encode("token"));
    const envelope = codec.decode(frame);
    assert.equal(envelope.messageKind, MessageKind.Authenticate);
    assert.equal(envelope.controlType, ControlPayload.AuthenticatePayload);
  });
});
