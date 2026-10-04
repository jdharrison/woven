import { strict as assert } from "node:assert";
import { describe, test } from "node:test";
import * as flatbuffers from "flatbuffers";
import {
  WovenClient, WovenConfig, encodeUnreliableEntityState,
} from "../src/index.js";
import {
  CAPABILITY_POSITIONED_ENTITY_STATE, DecodedEnvelope, EnvelopeCodec,
} from "../src/codec.js";
import { encodeEntityState, encodeReliableEvent } from "../src/encode.js";
import {
  WebTransport, WebTransportBidirectionalStream, WebTransportDatagramDuplexStream,
} from "../src/webtransport.js";
import {
  ControlPayload, DeliveryClass, Envelope as FbEnvelope, MessageKind,
} from "../generated/woven/protocol/v1.js";
import { buildAuthenticated, buildCapabilities, createEnvelope } from "./wire-helpers.js";

const codec = new EnvelopeCodec();
const scope = {
  namespaceId: 1n, sessionId: 2n, spaceId: 3n, spaceEpoch: 1n,
  channelId: 4n, entityId: 5n, senderSequence: 1n,
};
const body = new Uint8Array([10, 20, 30]);
const config: WovenConfig = {
  url: "https://localhost:4434/webtransport", token: "dev-token", connectTimeoutMs: 1_000,
};

function frame(sequence = 1n, bytes = body): Uint8Array {
  return encodeUnreliableEntityState({ ...scope, senderSequence: sequence }, { typeId: 6n, bytes });
}

function publish(client: WovenClient, bytes = body): Promise<void> {
  return client.publishUnreliableState(1n, 2n, 3n, 1n, 4n, 5n, 1n, 6n, bytes);
}

function malformedFrame(options: {
  kind?: MessageKind; delivery?: DeliveryClass; protocolVersion?: number;
  namespaceId?: bigint; sessionId?: bigint; spaceId?: bigint; spaceEpoch?: bigint;
  channelId?: bigint; entityId?: bigint; sequence?: bigint; typeId?: bigint;
  controlType?: ControlPayload;
}): Uint8Array {
  const builder = new flatbuffers.Builder(256);
  const bytes = builder.createByteVector(body);
  const root = createEnvelope(
    builder, options.protocolVersion ?? 1, options.kind ?? MessageKind.EntityState,
    options.delivery ?? DeliveryClass.UnreliableSequenced,
    options.namespaceId ?? 1n, options.sessionId ?? 2n, options.spaceId ?? 3n,
    options.entityId ?? 5n, options.spaceEpoch ?? 1n, 0n, options.sequence ?? 1n,
    0n, options.typeId ?? 6n, bytes, options.controlType ?? ControlPayload.NONE,
    0, options.channelId ?? 4n,
  );
  FbEnvelope.finishSizePrefixedEnvelopeBuffer(builder, root);
  return builder.asUint8Array();
}

class FakeDatagrams implements WebTransportDatagramDuplexStream {
  maxDatagramSize = 1_200;
  incomingHighWaterMark = 0;
  outgoingHighWaterMark = 0;
  incomingMaxAge: number | null = null;
  outgoingMaxAge: number | null = null;
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  readonly writes: Uint8Array[] = [];
  pulls = 0;
  cancels = 0;
  blockWrite: Promise<void> | undefined;
  cancelNeverSettles = false;
  private controller!: ReadableStreamDefaultController<Uint8Array>;

  constructor() {
    this.readable = new ReadableStream<Uint8Array>({
      start: (controller) => { this.controller = controller; },
      pull: () => { this.pulls += 1; },
      cancel: () => {
        this.cancels += 1;
        if (this.cancelNeverSettles) return new Promise<void>(() => {});
      },
    }, { highWaterMark: 0 });
    this.writable = new WritableStream<Uint8Array>({
      write: async (value) => {
        this.writes.push(value.slice());
        await this.blockWrite;
      },
    }, { highWaterMark: 1 });
  }

  push(value: Uint8Array): void { this.controller.enqueue(value); }
  end(): void { this.controller.close(); }
  fail(): void { this.controller.error(new Error("datagram read failure")); }
}

class Harness {
  readonly datagrams = new FakeDatagrams();
  readonly requests: DecodedEnvelope[] = [];
  readonly stream: WebTransportBidirectionalStream;
  readonly transport: WebTransport;
  controlWrites = 0;
  closes = 0;
  private controlController!: ReadableStreamDefaultController<Uint8Array>;
  private resolveClosed!: () => void;
  private rejectClosed!: (error: Error) => void;
  private controlEnded = false;

  constructor(capabilities = buildCapabilities()) {
    const readable = new ReadableStream<Uint8Array>({
      start: (controller) => { this.controlController = controller; },
    });
    const writable = new WritableStream<Uint8Array>({
      write: (value) => {
        this.controlWrites += 1;
        const request = codec.decode(value);
        this.requests.push(request);
        if (request.messageKind === MessageKind.Hello) this.pushControl(capabilities);
        if (request.messageKind === MessageKind.Authenticate) this.pushControl(buildAuthenticated());
      },
    });
    this.stream = { readable, writable };
    this.transport = {
      ready: Promise.resolve(),
      closed: new Promise((resolve, reject) => {
        this.resolveClosed = () => resolve({ closeCode: 0 });
        this.rejectClosed = reject;
      }),
      reliability: "supports-unreliable",
      datagrams: this.datagrams,
      createBidirectionalStream: async () => this.stream,
      close: () => {
        this.closes += 1;
        this.endControl();
        this.resolveClosed();
      },
    };
  }

  connect(overrides: Partial<WovenConfig> = {}): Promise<WovenClient> {
    return WovenClient.fromTransport(this.transport, this.stream, { ...config, ...overrides });
  }

  pushControl(value: Uint8Array): void { this.controlController.enqueue(value); }
  endControl(): void {
    if (!this.controlEnded) this.controlController.close();
    this.controlEnded = true;
  }
  remoteClose(fail = false): void {
    if (fail) this.rejectClosed(new Error("session failed"));
    else this.resolveClosed();
  }
}

async function tick(): Promise<void> { await new Promise<void>((resolve) => setImmediate(resolve)); }

describe("WebTransport opaque datagram lane", () => {
  test("exports the fixed unreliable encoder, bounds queues, and makes zero reliable publish writes", async () => {
    const server = new Harness();
    const client = await server.connect({
      webTransportOptions: { requireUnreliable: true }, datagramMaxAgeMs: 250,
    });
    assert.equal(server.datagrams.incomingHighWaterMark, 8);
    assert.equal(server.datagrams.outgoingHighWaterMark, 1);
    assert.equal(server.datagrams.incomingMaxAge, 250);
    assert.equal(server.datagrams.outgoingMaxAge, 250);
    assert.equal(server.datagrams.pulls, 0, "no background reader");
    const controlWrites = server.controlWrites;
    await publish(client);
    assert.equal(server.controlWrites, controlWrites);
    assert.equal(server.datagrams.writes.length, 1);
    const decoded = codec.decode(server.datagrams.writes[0]!);
    assert.equal(decoded.messageKind, MessageKind.EntityState);
    assert.equal(decoded.deliveryClass, DeliveryClass.UnreliableSequenced);
    assert.equal(decoded.channelId, 4n);
    assert.equal(decoded.entityId, 5n);
    assert.equal(decoded.senderSequence, 1n);
    assert.equal(decoded.payloadTypeId, 6n);
    assert.deepEqual(decoded.payload, body);
    assert.equal(server.datagrams.writable.locked, false);
    client.close();
  });

  test("outgoing datagram sequences may start at zero, matching WVN1", async () => {
    const server = new Harness();
    const client = await server.connect();
    try {
      assert.equal(codec.decode(frame(0n)).senderSequence, 0n);
      // Paired with ts_unreliable_entity_state_with_initial_zero_sequence_decodes in Rust.
      assert.equal(Buffer.from(frame(0n)).toString("hex"),
        "840000003000000057564e310000000024004c0000004b004a003c0034002c0024001c0000000000000014001000000000000400240000000400000000000000000000003c000000060000000000000001000000000000000500000000000000030000000000000002000000000000000100000000000000000000000000040d030000000a141e00",
      );
      const { senderSequence: _sequence, ...initialScope } = scope;
      assert.equal(codec.decode(encodeUnreliableEntityState(
        initialScope, { typeId: 6n, bytes: body },
      )).senderSequence, 0n);
      for (const sequence of [0n, 1n, 0xffff_ffff_ffff_ffffn]) {
        await client.publishUnreliableState(1n, 2n, 3n, 1n, 4n, 5n, sequence, 6n, body);
        assert.equal(codec.decode(server.datagrams.writes.at(-1)!).senderSequence, sequence);
      }
      assert.equal(server.controlWrites, 2);
    } finally {
      client.close();
    }
  });

  test("incoming datagram sequences may start at zero, matching WVN1", async () => {
    const server = new Harness();
    const client = await server.connect();
    try {
      server.datagrams.push(malformedFrame({ sequence: 0n }));
      const received = await client.recvDatagram();
      assert.equal(received?.senderSequence, 0n);
      assert.deepEqual(received?.payload, body);
      assert.equal(server.closes, 0);
    } finally {
      client.close();
    }
  });

  test("positioned datagrams require negotiation and carry the 3D position", async () => {
    const unsupportedServer = new Harness();
    const unsupported = await unsupportedServer.connect();
    assert.equal(unsupported.supportsPositionedState(), false);
    await assert.rejects(
      unsupported.publishUnreliablePositionedState(
        1n, 2n, 3n, 1n, 4n, 5n, 1n, 6n,
        { x: -1, y: 2, z: 3.5 }, body,
      ),
      { kind: "protocol", message: "server did not negotiate positioned EntityState" },
    );
    assert.equal(unsupportedServer.datagrams.writes.length, 0);
    unsupported.close();

    const server = new Harness(buildCapabilities({
      capabilityBits: CAPABILITY_POSITIONED_ENTITY_STATE,
    }));
    const client = await server.connect();
    assert.equal(client.supportsPositionedState(), true);
    await client.publishUnreliablePositionedState(
      1n, 2n, 3n, 1n, 4n, 5n, 1n, 6n,
      { x: -1, y: 2, z: 3.5 }, body,
    );
    assert.equal(server.datagrams.writes.length, 1);
    const decoded = codec.decode(server.datagrams.writes[0]!);
    assert.equal(decoded.deliveryClass, DeliveryClass.UnreliableSequenced);
    assert.deepEqual(decoded.routingPosition, { x: -1, y: 2, z: 3.5 });
    client.close();
  });

  test("default queue ages are null, not tied to any domain update cadence", async () => {
    const server = new Harness();
    const client = await server.connect();
    assert.equal(server.datagrams.incomingMaxAge, null);
    assert.equal(server.datagrams.outgoingMaxAge, null);
    client.close();
  });

  test("checks the current MTU against the entire frame with no retry, fragmentation, or fallback", async () => {
    const server = new Harness();
    const client = await server.connect();
    const length = frame().byteLength;
    server.datagrams.maxDatagramSize = length;
    await publish(client);
    server.datagrams.maxDatagramSize = length - 1;
    await assert.rejects(publish(client), {
      kind: "transport",
      message: `encoded datagram frame length ${length} bytes exceeds maxDatagramSize ${length - 1} bytes`,
    });
    assert.equal(server.datagrams.writes.length, 1);
    assert.equal(server.controlWrites, 2, "only handshake writes");
    server.datagrams.maxDatagramSize = length;
    await publish(client);
    assert.equal(server.datagrams.writes.length, 2, "explicit later publish remains possible");
    client.close();
  });

  for (const source of ["configured", "server"] as const) {
    test(`honors the ${source} full-frame bound independently of payload and MTU`, async () => {
      const bytes = new Uint8Array(128);
      const length = frame(1n, bytes).byteLength;
      const limit = length - 1;
      const server = new Harness(buildCapabilities({
        maxFrameSize: source === "server" ? limit : 1_048_576, maxPayloadSize: 128,
      }));
      server.datagrams.maxDatagramSize = 100_000;
      const client = await server.connect({
        maxPayloadBytes: 128, maxFrameBytes: source === "configured" ? limit : 1_048_576,
      });
      await assert.rejects(publish(client, bytes), {
        kind: "protocol",
        message: `encoded datagram frame length ${length} bytes exceeds frame limit ${limit} bytes`,
      });
      assert.equal(server.datagrams.writes.length, 0);
      assert.equal(server.controlWrites, 2);
      await publish(client);
      client.close();
    });
  }

  for (const [name, configured, advertised, limit] of [
    ["default", undefined, 65_536, 65_536],
    ["smaller config", 128, 256, 128],
    ["smaller server", 256, 128, 128],
    ["larger limits", 262_144, 262_144, 65_536],
  ] as const) {
    test(`${name} payload bound rejects before serialization and leaves the connection usable`, async (t) => {
      const server = new Harness(buildCapabilities({ maxPayloadSize: advertised }));
      server.datagrams.maxDatagramSize = 100_000;
      const client = await server.connect({ maxPayloadBytes: configured });
      const vectors = t.mock.method(flatbuffers.Builder.prototype, "createByteVector", () => {
        assert.fail("oversized payload reached FlatBuffers serialization");
      });
      await assert.rejects(publish(client, new Uint8Array(limit + 1)), {
        kind: "protocol", message: `payload length ${limit + 1} bytes exceeds limit ${limit} bytes`,
      });
      assert.equal(vectors.mock.callCount(), 0);
      vectors.mock.restore();
      assert.equal(server.datagrams.writes.length, 0);
      assert.equal(server.controlWrites, 2);
      assert.equal(server.closes, 0);
      const bytes = new Uint8Array(limit + 2).fill(42).subarray(1, limit + 1);
      await publish(client, bytes);
      const decoded = new EnvelopeCodec(1_048_576, advertised).decode(server.datagrams.writes[0]!);
      assert.deepEqual(decoded.payload, bytes);
      client.close();
    });
  }

  test("encoder guards payload, all scope IDs, sequence, and type ID before serialization", (t) => {
    const vectors = t.mock.method(flatbuffers.Builder.prototype, "createByteVector", () => {
      assert.fail("invalid update reached serialization");
    });
    assert.throws(() => frame(1n, new Uint8Array(65_537)), /payload length 65537/);
    for (const field of Object.keys(scope) as (keyof typeof scope)[]) {
      if (field === "senderSequence") continue;
      for (const value of [0n, -1n, 0x1_0000_0000_0000_0000n]) {
        assert.throws(() => encodeUnreliableEntityState(
          { ...scope, [field]: value }, { typeId: 6n, bytes: body },
        ), /must be a nonzero u64/);
      }
    }
    for (const sequence of [-1n, 0x1_0000_0000_0000_0000n]) {
      assert.throws(() => frame(sequence), /sender sequence must be a u64/);
    }
    for (const typeId of [0n, -1n, 0x1_0000_0000_0000_0000n]) {
      assert.throws(() => encodeUnreliableEntityState(scope, { typeId, bytes: body }), /nonzero u64/);
    }
    assert.equal(vectors.mock.callCount(), 0);
  });

  test("client normalizes invalid outgoing sequence/type/scope to the existing error shape", async () => {
    const server = new Harness();
    const client = await server.connect();
    for (const args of [
      [0n, 2n, 3n, 1n, 4n, 5n, 1n, 6n],
      [1n, 2n, 3n, 1n, 4n, 5n, -1n, 6n],
      [1n, 2n, 3n, 1n, 4n, 5n, 1n, 0n],
    ] as const) {
      const [namespaceId, sessionId, spaceId, epoch, channelId, entityId, sequence, typeId] = args;
      await assert.rejects(client.publishUnreliableState(
        namespaceId, sessionId, spaceId, epoch, channelId, entityId, sequence, typeId, body,
      ), (error: unknown) => {
        assert.deepEqual(Object.keys(error as object).sort(), ["kind", "message"]);
        return (error as { kind: string }).kind === "protocol";
      });
    }
    assert.equal(server.datagrams.writes.length, 0);
    assert.equal(server.controlWrites, 2);
    client.close();
  });

  test("datagram reads and writes remain independent of concurrent control reads and writes", async () => {
    const server = new Harness();
    const client = await server.connect();
    const controlReceive = client.recv();
    const datagramReceive = client.recvDatagram();
    let unblock!: () => void;
    server.datagrams.blockWrite = new Promise((resolve) => { unblock = resolve; });
    const datagramWrite = publish(client);
    await tick();
    await client.publishState(1n, 2n, 3n, 1n, 7n, 5n, 1n, 6n, body);
    await assert.rejects(publish(client), { kind: "transport", message: "another datagram write is already active" });
    await assert.rejects(client.recvDatagram(), { kind: "transport", message: "another datagram receive is already active" });
    server.pushControl(encodeReliableEvent(scope, { typeId: 6n, bytes: body }));
    server.datagrams.push(frame());
    assert.equal((await controlReceive).messageKind, MessageKind.ReliableEvent);
    assert.equal((await datagramReceive)?.deliveryClass, DeliveryClass.UnreliableSequenced);
    assert.equal(server.controlWrites, 3);
    assert.equal(server.datagrams.writes.length, 1);
    unblock();
    await datagramWrite;
    client.close();
  });

  test("repeated timeouts reuse exactly one read, including an already-resolved retained packet", async () => {
    const server = new Harness();
    const client = await server.connect();
    for (let count = 0; count < 5; count += 1) {
      assert.equal(await client.recvDatagramTimeout(1), null);
      assert.equal(server.datagrams.pulls, 1);
    }
    server.datagrams.push(frame(7n));
    await tick();
    assert.equal(server.datagrams.readable.locked, false);
    assert.equal(server.datagrams.pulls, 1, "no decoded inbox or background draining");
    server.datagrams.push(frame(2n));
    assert.equal((await client.recvDatagramTimeout(100))?.senderSequence, 7n);
    assert.equal((await client.recvDatagram())?.senderSequence, 2n, "client does not claim/provide ordering");
    client.close();
  });

  test("timeout polling does not accumulate handlers on the retained read promise", async (t) => {
    const server = new Harness();
    const client = await server.connect();
    assert.equal(await client.recvDatagramTimeout(0), null);
    // A single transport read alone would not detect unbounded promise reaction retention.
    const retained = (client as unknown as {
      pendingDatagramReceive: { promise: Promise<DecodedEnvelope | null> };
    }).pendingDatagramReceive.promise;
    const observer = t.mock.method(retained, "then");
    for (let count = 0; count < 20; count += 1) {
      assert.equal(await client.recvDatagramTimeout(0), null);
    }
    assert.equal(observer.mock.callCount(), 0, "one initial observer, no new handlers per timeout");
    assert.equal(server.datagrams.pulls, 1);
    client.close();
  });

  test("a malformed retained packet is reported once, not lost after timeout", async () => {
    const server = new Harness();
    const client = await server.connect();
    assert.equal(await client.recvDatagramTimeout(0), null);
    server.datagrams.push(new Uint8Array([1, 2]));
    await tick();
    server.datagrams.push(frame());
    await assert.rejects(client.recvDatagramTimeout(100), { kind: "protocol" });
    assert.deepEqual((await client.recvDatagram())?.payload, body);
    assert.equal(server.closes, 0);
    client.close();
  });

  test("a reused timeout read still has only one active consumer", async () => {
    const server = new Harness();
    const client = await server.connect();
    assert.equal(await client.recvDatagramTimeout(1), null);
    const receive = client.recvDatagramTimeout(100);
    await assert.rejects(client.recvDatagramTimeout(1), { kind: "transport", message: "another datagram receive is already active" });
    server.datagrams.push(frame());
    assert.equal((await receive)?.senderSequence, 1n);
    assert.equal(server.datagrams.pulls, 1);
    client.close();
  });

  test("rejects invalid timeouts before starting a read", async () => {
    const server = new Harness();
    const client = await server.connect();
    for (const ms of [-1, NaN, Infinity, 2_147_483_648]) {
      await assert.rejects(client.recvDatagramTimeout(ms), { kind: "transport" });
    }
    assert.equal(server.datagrams.pulls, 0);
    assert.equal(await client.recvDatagramTimeout(0), null);
    client.close();
  });

  test("malformed packets consume one read and reject without affecting control or later datagrams", async () => {
    const server = new Harness();
    const client = await server.connect();
    const invalid = [
      new Uint8Array([1, 2]),
      frame().subarray(0, frame().byteLength - 1),
      new Uint8Array([...frame(), 0]),
      encodeEntityState(scope, { typeId: 6n, bytes: body }),
      encodeReliableEvent(scope, { typeId: 6n, bytes: body }),
      buildCapabilities(),
      ...[
        { kind: MessageKind.ReliableEvent }, { delivery: DeliveryClass.Unknown },
        { protocolVersion: 2 }, { namespaceId: 0n }, { sessionId: 0n }, { spaceId: 0n },
        { spaceEpoch: 0n }, { channelId: 0n }, { entityId: 0n }, { typeId: 0n },
        { controlType: ControlPayload.HelloPayload },
      ].map(malformedFrame),
    ];
    for (const value of invalid) {
      server.datagrams.push(value);
      await assert.rejects(client.recvDatagram(), (error: unknown) => {
        const value = error as { kind: string; message: string };
        return value.kind === "protocol" && value.message.startsWith("invalid datagram (packet discarded):");
      });
    }
    assert.equal(server.closes, 0);
    server.datagrams.push(frame());
    assert.deepEqual((await client.recvDatagram())?.payload, body);
    server.pushControl(encodeReliableEvent(scope, { typeId: 6n, bytes: body }));
    assert.equal((await client.recv()).messageKind, MessageKind.ReliableEvent);
    client.close();
  });

  for (const bound of ["frame", "payload"] as const) {
    test(`enforces incoming per-packet codec ${bound} limits without accumulating datagram fragments`, async () => {
      const server = new Harness();
      const client = await server.connect({ maxFrameBytes: 256, maxPayloadBytes: 128 });
      server.datagrams.push(frame(1n, new Uint8Array(bound === "frame" ? 256 : 129)));
      await assert.rejects(client.recvDatagram(), { kind: "protocol" });
      const packet = frame();
      server.datagrams.push(packet.subarray(0, 4));
      server.datagrams.push(packet.subarray(4));
      await assert.rejects(client.recvDatagram(), { kind: "protocol" });
      await assert.rejects(client.recvDatagram(), { kind: "protocol" });
      server.datagrams.push(packet);
      assert.deepEqual((await client.recvDatagram())?.payload, body);
      client.close();
    });
  }

  test("surfaces read/write transport errors without reliable fallback", async () => {
    const server = new Harness();
    const client = await server.connect();
    server.datagrams.fail();
    await assert.rejects(client.recvDatagram(), { kind: "transport", message: "datagram receive failed: datagram read failure" });
    server.datagrams.blockWrite = Promise.reject(new Error("datagram write failure"));
    await assert.rejects(publish(client), { kind: "transport", message: "datagram write failed: datagram write failure" });
    assert.equal(server.controlWrites, 2);
    assert.equal(server.datagrams.writes.length, 1);
    client.close();
  });

  for (const mode of ["active", "timed-out", "never-settling-cancel", "graceful", "remote", "remote-failure"] as const) {
    test(`${mode} closure settles/cancels a pending datagram read without waiting for a packet`, async () => {
      const server = new Harness();
      const client = await server.connect();
      let receive: Promise<DecodedEnvelope | null> | undefined;
      if (mode === "timed-out") assert.equal(await client.recvDatagramTimeout(1), null);
      else receive = client.recvDatagram();
      if (mode === "never-settling-cancel") server.datagrams.cancelNeverSettles = true;
      if (mode === "remote" || mode === "remote-failure") server.remoteClose(mode === "remote-failure");
      else if (mode === "graceful") await client.closeGracefully(100);
      else client.close();
      if (receive) assert.equal(await receive, null);
      await tick();
      assert.equal(server.datagrams.cancels, 1);
      assert.equal(server.datagrams.readable.locked, false);
      assert.equal(server.datagrams.pulls, 1);
      if (mode === "remote" || mode === "remote-failure") assert.equal(await client.recvDatagram(), null);
      else await assert.rejects(client.recvDatagram(), { kind: "closed" });
      client.close();
    });
  }

  test("lane end returns null and prevents more writes", async () => {
    const server = new Harness();
    const client = await server.connect();
    server.datagrams.end();
    assert.equal(await client.recvDatagram(), null);
    assert.equal(await client.recvDatagramTimeout(1), null);
    await assert.rejects(publish(client), { kind: "closed" });
    client.close();
  });

  test("closure bounds an in-flight write as well as read", async () => {
    const server = new Harness();
    const client = await server.connect();
    server.datagrams.blockWrite = new Promise(() => {});
    const write = publish(client);
    const rejection = assert.rejects(write, { kind: "closed" });
    await tick();
    client.close();
    await rejection;
    assert.equal(server.datagrams.writable.locked, false);
  });

  for (const mode of ["exchange", "runner"] as const) {
    test(`rejects all datagram API use during managed admission ${mode}`, async () => {
      const server = new Harness();
      const client = await server.connect();
      const admission = mode === "exchange"
        ? client.requestAdmission(1n, 2n, 1n, "test")
        : client.admitWithCancellation(1n, 2n, "test", 1_000, new AbortController().signal);
      const rejection = assert.rejects(admission);
      await assert.rejects(publish(client), { kind: "transport", message: "datagram lane is unavailable during managed admission" });
      await assert.rejects(client.recvDatagram(), { kind: "transport" });
      await assert.rejects(client.recvDatagramTimeout(1), { kind: "transport" });
      assert.equal(server.datagrams.pulls, 0);
      assert.equal(server.datagrams.writes.length, 0);
      client.close();
      await rejection;
    });
  }

  test("admission cannot start while a datagram read, retained packet, or write owns the lane", async () => {
    const server = new Harness();
    const client = await server.connect();
    assert.equal(await client.recvDatagramTimeout(1), null);
    await assert.rejects(client.requestAdmission(1n, 2n, 1n, "test"), { kind: "transport" });
    server.datagrams.push(frame());
    await tick();
    await assert.rejects(client.admitWithCancellation(1n, 2n, "test", 1_000, new AbortController().signal), { kind: "transport" });
    await client.recvDatagram();
    server.datagrams.blockWrite = new Promise(() => {});
    const write = publish(client);
    const rejection = assert.rejects(write, { kind: "closed" });
    await assert.rejects(client.requestAdmission(1n, 2n, 1n, "test"), { kind: "transport" });
    assert.equal(server.controlWrites, 2);
    client.close();
    await rejection;
  });

  for (const unavailable of ["missing", "no-reader", "no-writer", "no-size", "zero-size", "reliable-only", "pending"] as const) {
    test(`${unavailable} transport rejects lane APIs rather than falling back`, async () => {
      const server = new Harness();
      const transport = server.transport as unknown as Record<string, unknown>;
      const datagrams = server.datagrams as unknown as Record<string, unknown>;
      if (unavailable === "missing") delete transport.datagrams;
      if (unavailable === "no-reader") delete datagrams.readable;
      if (unavailable === "no-writer") delete datagrams.writable;
      if (unavailable === "no-size") delete datagrams.maxDatagramSize;
      if (unavailable === "zero-size") datagrams.maxDatagramSize = 0;
      if (unavailable === "reliable-only" || unavailable === "pending") transport.reliability = unavailable;
      const client = await server.connect();
      await assert.rejects(publish(client), { kind: "transport", message: "unreliable WebTransport datagrams are not available" });
      await assert.rejects(client.recvDatagram(), { kind: "transport" });
      await assert.rejects(client.recvDatagramTimeout(1), { kind: "transport" });
      assert.equal(server.controlWrites, 2);
      assert.equal(server.datagrams.writes.length, 0);
      await client.publishEvent(1n, 2n, 3n, 1n, 7n, 5n, 1n, 6n, body);
      client.close();
    });
  }

  test("requireUnreliable also rejects unavailable embedded transports during connection", async () => {
    const server = new Harness();
    server.datagrams.maxDatagramSize = 0;
    await assert.rejects(server.connect({ webTransportOptions: { requireUnreliable: true } }), { kind: "transport" });
    assert.equal(server.closes, 1);
  });

  test("invalid optional max ages reject before any handshake writes", async () => {
    for (const datagramMaxAgeMs of [0, -1, NaN, Infinity, 2_147_483_648]) {
      const server = new Harness();
      await assert.rejects(server.connect({ datagramMaxAgeMs }), { kind: "transport" });
      assert.equal(server.controlWrites, 0);
    }
  });

  test("a runtime unable to apply bounded queue setters fails closed", async () => {
    const server = new Harness();
    Object.defineProperty(server.datagrams, "outgoingHighWaterMark", { writable: false });
    await assert.rejects(server.connect(), { kind: "transport" });
    assert.equal(server.closes, 1);
    assert.equal(server.datagrams.writes.length, 0);
  });
});
