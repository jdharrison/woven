import { test, describe, beforeEach } from "node:test";
import { strict as assert } from "node:assert";
import { readFileSync } from "node:fs";
import * as flatbuffers from "flatbuffers";
import { WovenClient } from "../src/client.js";
import {
  CAPABILITY_POSITIONED_ENTITY_STATE,
  EnvelopeCodec,
  DecodedEnvelope,
} from "../src/codec.js";
import { MessageKind, ControlPayload, HelloPayload, InferenceRequestedPayload } from "../generated/woven/protocol/v1.js";
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
  encodeEntityState,
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
  writeCount = 0;
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
        self.writeCount += 1;
        self.ingest(new Uint8Array(chunk));
      },
    });
    this.bidi = { readable: this.readable, writable: this.writable };
  }

  pushFrame(frame: Uint8Array): void {
    if (this.controller) this.controller.enqueue(frame);
    else this.pushQueue.push(frame);
  }

  closeStream(): void {
    this.controller?.close();
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

function makeWebTransport(
  server: FakeServer,
  onClose: () => void = () => {},
  closed: Promise<{ closeCode?: number; reason?: string }> = Promise.resolve({ closeCode: 0 }),
): WebTransport {
  return {
    ready: Promise.resolve(),
    closed,
    datagrams: {
      maxDatagramSize: 1_200,
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
    const packageVersion = (JSON.parse(
      readFileSync(new URL("../package.json", import.meta.url), "utf8"),
    ) as { version: string }).version;
    assert.equal((server.requests[0]!.control as HelloPayload).clientVersion(), packageVersion);
    assert.equal((codec.decode(encodeHello({})).control as HelloPayload).clientVersion(), packageVersion);
    client.close();
  });

  test("negotiates and publishes reliable positioned state, rejecting it when absent", async () => {
    const unsupportedServer = new FakeServer();
    const unsupported = await WovenClient.fromTransport(
      makeWebTransport(unsupportedServer),
      unsupportedServer.bidi,
      { url: "https://localhost:4433/webtransport", token: "dev-token" },
    );
    assert.equal(unsupported.supportsPositionedState(), false);
    const writesBefore = unsupportedServer.writeCount;
    await assert.rejects(
      unsupported.publishPositionedState(
        1n, 1n, 3n, 1n, 1n, 7n, 1n, 5n,
        { x: 1, y: 2, z: 3 }, encoder.encode("state"),
      ),
      { kind: "protocol", message: "server did not negotiate positioned EntityState" },
    );
    assert.equal(unsupportedServer.writeCount, writesBefore);
    unsupported.close();

    const positionedServer = new FakeServer(
      true,
      true,
      buildCapabilities({ capabilityBits: CAPABILITY_POSITIONED_ENTITY_STATE }),
    );
    const positioned = await WovenClient.fromTransport(
      makeWebTransport(positionedServer),
      positionedServer.bidi,
      { url: "https://localhost:4433/webtransport", token: "dev-token" },
    );
    assert.equal(positioned.supportsPositionedState(), true);
    await positioned.publishPositionedState(
      1n, 1n, 3n, 1n, 1n, 7n, 1n, 5n,
      { x: 1, y: 2, z: 3 }, encoder.encode("state"),
    );
    const published = positionedServer.requests.at(-1)!;
    assert.equal(published.messageKind, MessageKind.EntityState);
    assert.deepEqual(published.routingPosition, { x: 1, y: 2, z: 3 });
    positioned.close();
  });

  for (const method of ["connect", "fromTransport"] as const) {
    test(`${method} normalizes oversized credentials without writing Authenticate`, async () => {
      let closeCount = 0;
      const transport = makeWebTransport(server, () => { closeCount += 1; });
      if (method === "connect") {
        (globalThis as Record<string, unknown>).WebTransport = function (_url: string) {
          return transport;
        } as unknown as typeof globalThis.WebTransport;
      }
      try {
        const config = {
          url: "https://localhost:4433/webtransport",
          token: "é".repeat(32_768) + "x",
          connectTimeoutMs: 1_000,
        };
        await assert.rejects(
          method === "connect"
            ? WovenClient.connect(config)
            : WovenClient.fromTransport(transport, server.bidi, config),
          { kind: "protocol", message: "payload length 65537 bytes exceeds limit 65536 bytes" },
        );
        assert.equal(server.writeCount, 1, "only Hello may be written");
        assert.deepEqual(server.requests.map((request) => request.messageKind), [MessageKind.Hello]);
        assert.equal(closeCount, 1, "failed handshake closes the transport exactly once");
      } finally {
        if (method === "connect") delete (globalThis as Record<string, unknown>).WebTransport;
      }
    });
  }

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

  test("requestInference normalizes oversized control payloads and leaves the connection usable", async () => {
    let closeCount = 0;
    const client = await WovenClient.fromTransport(
      makeWebTransport(server, () => { closeCount += 1; }),
      server.bidi,
      { url: "https://localhost:4433/webtransport", token: "dev-token" },
    );
    try {
      const writesBefore = server.writeCount;
      const requestsBefore = server.requests.length;
      await assert.rejects(
        client.requestInference(1n, 1n, 1n, 1n, 9n, "é", 500n, new Uint8Array(65_535)),
        { kind: "protocol", message: "payload length 65537 bytes exceeds limit 65536 bytes" },
      );
      assert.equal(server.writeCount, writesBefore, "no transport write for rejected inference");
      assert.equal(server.requests.length, requestsBefore);
      assert.equal(closeCount, 0);

      const input = encoder.encode("q");
      await client.requestInference(1n, 1n, 1n, 1n, 9n, "é", 500n, input);
      assert.equal(server.writeCount, writesBefore + 1);
      const request = server.requests.at(-1)!;
      assert.equal(request.messageKind, MessageKind.InferenceRequested);
      assert.deepEqual((request.control as InferenceRequestedPayload).inputArray(), input);
      const reply = encoder.encode("still connected");
      server.pushFrame(encodeReliableEvent(
        { namespaceId: 1n, sessionId: 1n, spaceId: 1n, spaceEpoch: 1n, channelId: 1n, entityId: 5n, senderSequence: 1n },
        { typeId: 1n, bytes: reply },
      ));
      assert.deepEqual((await client.recv()).payload, reply);
      assert.equal(closeCount, 0);
    } finally {
      client.close();
    }
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

describe("WovenClient publishing payload bounds", () => {
  const cases = [
    { name: "defaults", config: {}, serverLimit: 65_536, limit: 65_536 },
    { name: "smaller configured limit", config: { maxPayloadBytes: 128 }, serverLimit: 256, limit: 128 },
    { name: "smaller server limit", config: { maxPayloadBytes: 256 }, serverLimit: 128, limit: 128 },
    { name: "larger server limit", config: {}, serverLimit: 262_144, limit: 65_536 },
    { name: "larger configured and server limits", config: { maxPayloadBytes: 262_144 }, serverLimit: 262_144, limit: 65_536 },
  ];
  const publishers = [
    { method: "publishState" as const, kind: MessageKind.EntityState, encode: encodeEntityState },
    { method: "publishEvent" as const, kind: MessageKind.ReliableEvent, encode: encodeReliableEvent },
  ];

  for (const { method, kind, encode } of publishers) {
    for (const { name, config, serverLimit, limit } of cases) {
      test(`${method}: ${name} accepts max, rejects max + 1, and remains usable`, async (t) => {
        const server = new FakeServer(true, true, buildCapabilities({ maxPayloadSize: serverLimit }));
        let closeCount = 0;
        const client = await WovenClient.fromTransport(
          makeWebTransport(server, () => { closeCount += 1; }),
          server.bidi,
          { url: "https://localhost:4433/webtransport", token: "dev-token", ...config },
        );
        try {
          const writesBefore = server.writeCount;
          const requestsBefore = server.requests.length;
          const serialization = t.mock.method(flatbuffers.Builder.prototype, "createByteVector", () => {
            assert.fail("oversized payload reached FlatBuffers serialization");
          });
          await assert.rejects(
            client[method](1n, 1n, 1n, 1n, 1n, 1n, 1n, 1n, new Uint8Array(limit + 1)),
            (error: unknown) => {
              const value = error as { kind?: string; message?: string };
              return value.kind === "protocol" &&
                value.message === `payload length ${limit + 1} bytes exceeds limit ${limit} bytes`;
            },
          );
          assert.equal(serialization.mock.calls.length, 0);
          serialization.mock.restore();
          assert.equal(server.writeCount, writesBefore, "no transport writes on rejection");
          assert.equal(server.requests.length, requestsBefore);
          assert.equal(closeCount, 0, "local payload rejection does not close the connection");

          const payload = new Uint8Array(limit + 2).fill(42).subarray(1, limit + 1);
          await client[method](1n, 1n, 1n, 1n, 1n, 1n, 1n, 1n, payload);
          assert.equal(server.writeCount, writesBefore + 1);
          const published = server.requests.at(-1)!;
          assert.equal(published.messageKind, kind);
          assert.deepEqual(published.payload, payload);

          const frame = encode(
            { namespaceId: 1n, sessionId: 1n, spaceId: 1n, spaceEpoch: 1n, channelId: 1n, entityId: 1n, senderSequence: 1n },
            { typeId: 1n, bytes: payload },
          );
          assert.ok(frame.byteLength > limit, "frame includes overhead beyond payload bytes");
          server.pushFrame(frame);
          assert.deepEqual((await client.recv()).payload, payload);
          assert.equal(closeCount, 0);
        } finally {
          client.close();
        }
      });
    }
  }
});

describe("WovenClient outgoing frame bounds", () => {
  const scope = {
    namespaceId: 1n, sessionId: 1n, spaceId: 3n, spaceEpoch: 1n,
    channelId: 2n, entityId: 7n, senderSequence: 1n,
  };
  const position = { x: 1, y: 2, z: 3 };
  const publishers = [
    {
      name: "publishEvent",
      encode: (bytes: Uint8Array) => encodeReliableEvent(scope, { typeId: 5n, bytes }),
      publish: (client: WovenClient, bytes: Uint8Array) =>
        client.publishEvent(1n, 1n, 3n, 1n, 2n, 7n, 1n, 5n, bytes),
    },
    {
      name: "publishState",
      encode: (bytes: Uint8Array) => encodeEntityState(scope, { typeId: 5n, bytes }),
      publish: (client: WovenClient, bytes: Uint8Array) =>
        client.publishState(1n, 1n, 3n, 1n, 2n, 7n, 1n, 5n, bytes),
    },
    {
      name: "publishPositionedState",
      encode: (bytes: Uint8Array) => encodeEntityState(
        { ...scope, routingPosition: position }, { typeId: 5n, bytes },
      ),
      publish: (client: WovenClient, bytes: Uint8Array) =>
        client.publishPositionedState(1n, 1n, 3n, 1n, 2n, 7n, 1n, 5n, position, bytes),
    },
  ];

  for (const { name, encode, publish } of publishers) {
    for (const bound of ["configured", "server"] as const) {
      test(`${name} enforces the ${bound} complete-frame limit without writing or closing`, async () => {
        const payload = new Uint8Array(32);
        const limit = encode(payload).byteLength;
        const server = new FakeServer(true, true, buildCapabilities({
          maxFrameSize: bound === "server" ? limit : 1_048_576,
          maxPayloadSize: 128,
          capabilityBits: CAPABILITY_POSITIONED_ENTITY_STATE,
        }));
        let closeCount = 0;
        const client = await WovenClient.fromTransport(
          makeWebTransport(server, () => { closeCount += 1; }), server.bidi,
          {
            url: "https://localhost:4433/webtransport", token: "dev-token",
            maxFrameBytes: bound === "configured" ? limit : 1_048_576,
            maxPayloadBytes: 128,
          },
        );
        try {
          const writesBefore = server.writeCount;
          const oversized = new Uint8Array(128);
          assert.ok(encode(oversized).byteLength > limit);
          await assert.rejects(publish(client, oversized), {
            kind: "protocol",
            message: `encoded frame length ${encode(oversized).byteLength} bytes exceeds frame limit ${limit} bytes`,
          });
          assert.equal(server.writeCount, writesBefore);
          assert.equal(closeCount, 0);
          await publish(client, payload);
          assert.equal(server.writeCount, writesBefore + 1);
          assert.deepEqual(server.requests.at(-1)!.payload, payload);
          assert.equal(closeCount, 0);
        } finally {
          client.close();
        }
      });
    }
  }

  test("negotiated frame limits reject oversized Authenticate before writing it", async () => {
    const server = new FakeServer(true, true, buildCapabilities({ maxFrameSize: 256, maxPayloadSize: 256 }));
    let closeCount = 0;
    const token = "x".repeat(256);
    await assert.rejects(WovenClient.fromTransport(
      makeWebTransport(server, () => { closeCount += 1; }), server.bidi,
      { url: "https://localhost:4433/webtransport", token },
    ), {
      kind: "protocol",
      message: `encoded frame length ${encodeAuthenticate(encoder.encode(token)).byteLength} bytes exceeds frame limit 256 bytes`,
    });
    assert.deepEqual(server.requests.map((request) => request.messageKind), [MessageKind.Hello]);
    assert.equal(closeCount, 1);
  });
});

describe("WovenClient graceful close", () => {
  test("waits for transport closure and closes exactly once", async () => {
    const server = new FakeServer();
    let closeCount = 0;
    let resolveClosed!: (value: { closeCode: number }) => void;
    const closed = new Promise<{ closeCode: number }>((resolve) => {
      resolveClosed = resolve;
    });
    const client = await WovenClient.fromTransport(
      makeWebTransport(
        server,
        () => {
          closeCount += 1;
        },
        closed,
      ),
      server.bidi,
      { url: "https://localhost:4433/webtransport", token: "dev-token" },
    );

    let settled = false;
    const closing = client.closeGracefully().then(() => {
      settled = true;
    });
    await Promise.resolve();
    assert.equal(closeCount, 1);
    assert.equal(settled, false);

    resolveClosed({ closeCode: 0 });
    await closing;
    await client.closeGracefully();
    client.close();
    assert.equal(closeCount, 1);
  });

  test("still closes the transport after the control stream ends", async () => {
    const server = new FakeServer();
    let closeCount = 0;
    let resolveClosed!: (value: { closeCode: number }) => void;
    const closed = new Promise<{ closeCode: number }>((resolve) => {
      resolveClosed = resolve;
    });
    const client = await WovenClient.fromTransport(
      makeWebTransport(
        server,
        () => {
          closeCount += 1;
          resolveClosed({ closeCode: 0 });
        },
        closed,
      ),
      server.bidi,
      { url: "https://localhost:4433/webtransport", token: "dev-token" },
    );

    server.closeStream();
    await assert.rejects(client.recv(), (error: unknown) => {
      const value = error as { kind?: string; message?: string };
      return value.kind === "closed" && value.message === "control stream ended";
    });
    await client.closeGracefully();
    assert.equal(closeCount, 1);
  });

  test("allows close initiation to be retried after a synchronous runtime exception", async () => {
    const server = new FakeServer();
    let closeCount = 0;
    const client = await WovenClient.fromTransport(
      makeWebTransport(server, () => {
        closeCount += 1;
        if (closeCount === 1) throw new Error("runtime close failed");
      }),
      server.bidi,
      { url: "https://localhost:4433/webtransport", token: "dev-token" },
    );

    assert.throws(() => client.close(), /runtime close failed/);
    client.close();
    assert.equal(closeCount, 2);
  });

  test("returns a bounded Woven transport error when closure stalls", async () => {
    const server = new FakeServer();
    let closeCount = 0;
    const client = await WovenClient.fromTransport(
      makeWebTransport(
        server,
        () => {
          closeCount += 1;
        },
        new Promise(() => {}),
      ),
      server.bidi,
      { url: "https://localhost:4433/webtransport", token: "dev-token" },
    );

    await assert.rejects(client.closeGracefully(10), (error: unknown) => {
      const value = error as { kind?: string; message?: string };
      return value.kind === "transport" && value.message === "WebTransport close timed out after 10ms";
    });
    client.close();
    assert.equal(closeCount, 1);
  });

  test("normalizes transport closure failures", async () => {
    const server = new FakeServer();
    let rejectClosed!: (reason: Error) => void;
    const closed = new Promise<{ closeCode: number }>((_resolve, reject) => {
      rejectClosed = reject;
    });
    const client = await WovenClient.fromTransport(
      makeWebTransport(server, () => {}, closed),
      server.bidi,
      { url: "https://localhost:4433/webtransport", token: "dev-token" },
    );

    const closing = client.closeGracefully();
    rejectClosed(new Error("runtime failure\nwith detail"));
    await assert.rejects(closing, (error: unknown) => {
      const value = error as { kind?: string; message?: string };
      return (
        value.kind === "transport" &&
        value.message === "WebTransport close failed: runtime failure with detail"
      );
    });
  });

  test("rejects invalid timeouts before closing the client", async () => {
    const server = new FakeServer();
    let closeCount = 0;
    const client = await WovenClient.fromTransport(
      makeWebTransport(server, () => {
        closeCount += 1;
      }),
      server.bidi,
      { url: "https://localhost:4433/webtransport", token: "dev-token" },
    );

    for (const timeoutMs of [0, -1, Number.NaN, Number.POSITIVE_INFINITY, 10_001]) {
      await assert.rejects(
        client.closeGracefully(timeoutMs),
        /timeoutMs must be a positive finite number no greater than 10000/,
      );
    }
    assert.equal(closeCount, 0);
    await client.joinSession(1n, 1n);
    client.close();
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
