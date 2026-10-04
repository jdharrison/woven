import { strict as assert } from "node:assert";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import * as flatbuffers from "flatbuffers";
import {
  AdmissionRejectionCode, AdmissionResultPayload, AdmissionStatus, ClientLogPayload,
  ControlPayload, DeliveryClass, Envelope as FbEnvelope, LogLevel, MessageKind,
  QueueState, QueueUpdatePayload,
} from "../generated/woven/protocol/v1.js";
import {
  CAPABILITY_CLIENT_LOG, CAPABILITY_POSITIONED_ENTITY_STATE, ClientLogger, DecodedEnvelope,
  encodeClientLog, encodeReliableEvent,
  EnvelopeCodec, MAX_LOG_MESSAGE_BYTES, WovenClient,
} from "../src/index.js";
import { WebTransport, WebTransportBidirectionalStream } from "../src/webtransport.js";
import { buildAuthenticated, buildCapabilities, buildProtocolError } from "./wire-helpers.js";

const codec = new EnvelopeCodec();
const scope = { namespaceId: 11n, sessionId: 22n };

type WireScope = {
  namespaceId?: bigint; sessionId?: bigint; spaceId?: bigint; spaceEpoch?: bigint;
  channelId?: bigint; entityId?: bigint; correlationId?: bigint;
  deliveryClass?: DeliveryClass; payloadTypeId?: bigint;
};

function finish(
  builder: flatbuffers.Builder, kind: MessageKind, type: ControlPayload, control: number,
  options: WireScope = scope,
): Uint8Array {
  FbEnvelope.startEnvelope(builder);
  FbEnvelope.addProtocolVersion(builder, 1);
  FbEnvelope.addMessageKind(builder, kind);
  FbEnvelope.addDeliveryClass(builder, options.deliveryClass ?? DeliveryClass.ReliableOrdered);
  FbEnvelope.addNamespaceId(builder, options.namespaceId ?? 0n);
  FbEnvelope.addSessionId(builder, options.sessionId ?? 0n);
  FbEnvelope.addSpaceId(builder, options.spaceId ?? 0n);
  FbEnvelope.addSpaceEpoch(builder, options.spaceEpoch ?? 0n);
  FbEnvelope.addChannelId(builder, options.channelId ?? 0n);
  FbEnvelope.addEntityId(builder, options.entityId ?? 0n);
  FbEnvelope.addCorrelationId(builder, options.correlationId ?? 0n);
  FbEnvelope.addPayloadTypeId(builder, options.payloadTypeId ?? 0n);
  FbEnvelope.addControlType(builder, type);
  FbEnvelope.addControl(builder, control);
  FbEnvelope.finishSizePrefixedEnvelopeBuffer(builder, FbEnvelope.endEnvelope(builder));
  return builder.asUint8Array();
}

function rawLog(
  message = "hello", level: LogLevel = LogLevel.Info, options: WireScope = scope,
  kind = MessageKind.ClientLog,
): Uint8Array {
  const builder = new flatbuffers.Builder(1280);
  const text = builder.createString(message);
  const control = ClientLogPayload.createClientLogPayload(builder, level, text);
  return finish(builder, kind, ControlPayload.ClientLogPayload, control, options);
}

function admission(request: DecodedEnvelope, status: AdmissionStatus): Uint8Array {
  const builder = new flatbuffers.Builder(256);
  const control = AdmissionResultPayload.createAdmissionResultPayload(
    builder, status, status === AdmissionStatus.Rejected ? AdmissionRejectionCode.QueueFull : AdmissionRejectionCode.None,
    status === AdmissionStatus.Queued ? 7n : 0n, 0, 0,
  );
  return finish(builder, MessageKind.AdmissionResult, ControlPayload.AdmissionResultPayload, control, {
    namespaceId: request.namespaceId, sessionId: request.sessionId, correlationId: request.correlationId ?? 0n,
  });
}

function queue(request: DecodedEnvelope, state: QueueState): Uint8Array {
  const builder = new flatbuffers.Builder(256);
  const control = QueueUpdatePayload.createQueueUpdatePayload(
    builder, 7n, state, state === QueueState.Waiting ? 1 : 0, 0, 0, 0,
  );
  return finish(builder, MessageKind.QueueUpdate, ControlPayload.QueueUpdatePayload, control, {
    namespaceId: request.namespaceId, sessionId: request.sessionId, correlationId: request.correlationId ?? 0n,
  });
}

class LogPeer {
  readonly requests: DecodedEnvelope[] = [];
  readonly stream: WebTransportBidirectionalStream;
  readonly transport: WebTransport;
  blockWrite: Promise<void> | null = null;
  failWrite = false;
  bytesWritten = 0;
  closeCount = 0;
  private controller!: ReadableStreamDefaultController<Uint8Array>;
  private resolveClosed!: (value: object) => void;

  constructor(
    private readonly handler: (request: DecodedEnvelope, peer: LogPeer) => void = () => {},
    payloadLimit = 65_536,
    capabilityBits = CAPABILITY_CLIENT_LOG,
  ) {
    this.stream = {
      readable: new ReadableStream({ start: (controller) => { this.controller = controller; } }),
      writable: new WritableStream({ write: async (frame) => {
        if (this.failWrite) throw new Error("write failed");
        this.bytesWritten += frame.byteLength;
        if (this.blockWrite !== null) await this.blockWrite;
        const request = codec.decode(frame);
        this.requests.push(request);
        if (request.messageKind === MessageKind.Hello) this.push(buildCapabilities({ maxPayloadSize: payloadLimit, capabilityBits }));
        else if (request.messageKind === MessageKind.Authenticate) this.push(buildAuthenticated());
        else this.handler(request, this);
      } }),
    };
    this.transport = {
      ready: Promise.resolve(),
      closed: new Promise((resolve) => { this.resolveClosed = resolve; }),
      createBidirectionalStream: async () => this.stream,
      close: () => { this.closeCount += 1; this.end(); },
    } as WebTransport;
  }

  push(frame: Uint8Array): void { this.controller.enqueue(frame); }
  end(): void { this.resolveClosed({}); }
  async connect(): Promise<WovenClient> {
    return WovenClient.fromTransport(this.transport, this.stream, { url: "https://localhost/webtransport", token: "test" });
  }
}

function assertLog(request: DecodedEnvelope, level: LogLevel, message: string): void {
  assert.equal(request.messageKind, 40);
  assert.equal(request.controlType, 37);
  assert.equal(request.deliveryClass, DeliveryClass.ReliableOrdered);
  assert.equal(request.namespaceId, scope.namespaceId);
  assert.equal(request.sessionId, scope.sessionId);
  assert.equal(request.spaceId, 0n);
  assert.equal(request.spaceEpoch, 0n);
  assert.equal(request.channelId, null);
  assert.equal(request.entityId, null);
  assert.equal(request.payloadTypeId, 0n);
  assert.ok(request.control instanceof ClientLogPayload);
  assert.equal(request.control.level(), level);
  assert.equal(request.control.message(), message);
}

test("stable log values and UTF-8 boundaries", () => {
  assert.equal(CAPABILITY_CLIENT_LOG, 2n);
  assert.equal(MAX_LOG_MESSAGE_BYTES, 1024);
  assert.equal(LogLevel.Unknown, 0);
  for (const level of [LogLevel.Info, LogLevel.Warn, LogLevel.Error]) {
    for (const message of ["hello", "é".repeat(512), "🧶".repeat(256)]) {
      assertLog(codec.decode(encodeClientLog(scope, level, message)), level, message);
    }
  }
  for (const message of ["", "x".repeat(1025), "é".repeat(513), "🧶".repeat(256) + "x"]) {
    assert.throws(() => encodeClientLog(scope, LogLevel.Info, message), /1 to 1024 UTF-8 bytes/);
    assert.throws(() => codec.decode(rawLog(message)), /1 to 1024 UTF-8 bytes/);
  }
  for (const level of [LogLevel.Unknown, 255 as LogLevel]) {
    assert.throws(() => encodeClientLog(scope, level, "hello"), /log level/);
    assert.throws(() => codec.decode(rawLog("hello", level)), /log level/);
  }
});

test("log encode/decode reject bad session scope and decode rejects wrong delivery/union", () => {
  for (const bad of [
    { namespaceId: 0n }, { sessionId: 0n }, { spaceId: 1n }, { spaceEpoch: 1n },
    { channelId: 1n }, { entityId: 1n },
  ]) {
    assert.throws(() => encodeClientLog({ ...scope, ...bad }, LogLevel.Info, "hello"));
    assert.throws(() => codec.decode(rawLog("hello", LogLevel.Info, { ...scope, ...bad })));
  }
  assert.throws(() => codec.decode(rawLog("hello", LogLevel.Info, { ...scope, deliveryClass: DeliveryClass.ReliableUnordered })));
  assert.throws(() => codec.decode(rawLog("hello", LogLevel.Info, { ...scope, payloadTypeId: 1n })));
  assert.throws(() => codec.decode(rawLog("hello", LogLevel.Info, scope, MessageKind.JoinSession)));
  const frame = encodeClientLog(scope, LogLevel.Info, "é".repeat(8));
  assert.throws(() => new EnvelopeCodec(4096, 15).decode(frame), /payload length 16/);
});

test("logger remembers legacy join, sends all levels and alias without an acknowledgement", async () => {
  const peer = new LogPeer();
  const client = await peer.connect();
  const logger: ClientLogger = client.logger;
  await assert.rejects(logger.info("before join"), { kind: "protocol", message: "no joined or admitted session" });
  assert.equal(peer.requests.length, 2);
  await assert.rejects(client.joinSession(0n, 22n));
  await assert.rejects(logger.info("still not joined"));
  await client.joinSession(11n, 22n);
  await logger.info("info");
  await logger.warn("warn");
  await logger.error("error");
  await client.log("alias");
  for (const [index, level, message] of [[3, LogLevel.Info, "info"], [4, LogLevel.Warn, "warn"], [5, LogLevel.Error, "error"], [6, LogLevel.Info, "alias"]] as const) {
    assertLog(peer.requests[index]!, level, message);
  }
  await client.leaveSession();
  await assert.rejects(logger.info("after leave"));
  client.close();
});

test("unnegotiated logging writes no bytes and preserves session and ordinary traffic", async () => {
  for (const capabilityBits of [0n, CAPABILITY_POSITIONED_ENTITY_STATE]) {
    const peer = new LogPeer(undefined, 65_536, capabilityBits);
    const client = await peer.connect();
    await client.joinSession(scope.namespaceId, scope.sessionId);
    const bytesBeforeLogs = peer.bytesWritten;
    for (const send of [client.logger.info, client.logger.warn, client.logger.error, (message: string) => client.log(message)]) {
      await assert.rejects(send("unsupported"), { kind: "protocol", message: "server did not negotiate ClientLog" });
      assert.equal(peer.bytesWritten, bytesBeforeLogs, "unsupported logs must not write any bytes");
      assert.equal(peer.closeCount, 0, "unsupported logging must not close the connection");
    }
    const payload = new Uint8Array([42]);
    await client.publishEvent(11n, 22n, 3n, 1n, 1n, 6n, 1n, 8n, payload);
    const request = peer.requests.at(-1)!;
    assert.equal(request.messageKind, MessageKind.ReliableEvent);
    assert.deepEqual(request.payload, payload);
    peer.push(encodeReliableEvent(
      { ...scope, spaceId: 3n, spaceEpoch: 1n, channelId: 1n, entityId: 6n, senderSequence: 1n },
      { typeId: 8n, bytes: payload },
    ));
    assert.equal((await client.recv()).messageKind, MessageKind.ReliableEvent);
    await client.leaveSession();
    const leave = peer.requests.at(-1)!;
    assert.equal(leave.messageKind, MessageKind.LeaveSession);
    assert.equal(leave.namespaceId, scope.namespaceId);
    assert.equal(leave.sessionId, scope.sessionId, "unsupported logs must retain the joined session scope");
    assert.equal(peer.closeCount, 0);
    client.close();
  }
});

test("invalid messages, negotiated limits and concurrent writes do not queue or retry", async () => {
  const peer = new LogPeer(undefined, 64);
  const client = await peer.connect();
  await client.joinSession(11n, 22n);
  for (const message of ["", "é".repeat(33), "🧶".repeat(257)]) await assert.rejects(client.log(message));
  assert.equal(peer.requests.length, 3);
  let release!: () => void;
  peer.blockWrite = new Promise((resolve) => { release = resolve; });
  const sending = client.logger.info("first");
  await assert.rejects(client.logger.warn("second"), (error: { message: string }) => error.message.includes("write is already active"));
  release();
  await sending;
  assert.equal(peer.requests.length, 4);
  peer.blockWrite = null;
  await client.logger.error("third");
  assert.equal(peer.requests.length, 5);
  peer.failWrite = true;
  await assert.rejects(client.log("failed"));
  await assert.rejects(client.log("no retry"), { kind: "protocol", message: "no joined or admitted session" });
  client.close();
});

test("observed join rejection, close and transport closure invalidate log scope", async () => {
  for (const action of ["rejection", "close", "transport"] as const) {
    const peer = new LogPeer();
    const client = await peer.connect();
    await client.joinSession(11n, 22n);
    if (action === "rejection") {
      peer.push(buildProtocolError({ ...scope, relatedKind: MessageKind.JoinSession }));
      await client.recv();
    } else if (action === "close") client.close();
    else { peer.end(); await Promise.resolve(); }
    await assert.rejects(client.log("not valid"));
    assert.equal(peer.requests.length, 3);
    client.close();
  }
});

for (const status of [AdmissionStatus.Admitted, AdmissionStatus.Queued, AdmissionStatus.Paused, AdmissionStatus.Rejected]) {
  test(`direct admission remembers scope only when Admitted (${status})`, async () => {
    const peer = new LogPeer((request, server) => {
      if (request.messageKind === MessageKind.RequestAdmission) server.push(admission(request, status));
    });
    const client = await peer.connect();
    await client.requestAdmission(11n, 22n, 1n, "key");
    if (status === AdmissionStatus.Admitted) {
      await client.log("admitted");
      assertLog(peer.requests[3]!, LogLevel.Info, "admitted");
    } else await assert.rejects(client.log("not admitted"));
    client.close();
  });
}

test("queue claim remembers admitted session", async () => {
  const peer = new LogPeer((request, server) => {
    if (request.messageKind === MessageKind.QueueClaim) server.push(queue(request, QueueState.Admitted));
  });
  const client = await peer.connect();
  await client.queueClaim(11n, 22n, 1n, 7n);
  await client.logger.warn("claimed");
  assertLog(peer.requests[3]!, LogLevel.Warn, "claimed");
  client.close();
});

for (const queued of [false, true]) {
  test(`admission runner remembers scope including queue claim (queued=${queued})`, { timeout: 5000 }, async () => {
    const peer = new LogPeer((request, server) => {
      if (request.messageKind === MessageKind.RequestAdmission) server.push(admission(request, queued ? AdmissionStatus.Queued : AdmissionStatus.Admitted));
      else if (request.messageKind === MessageKind.QueueHeartbeat) server.push(queue(request, QueueState.Offered));
      else if (request.messageKind === MessageKind.QueueClaim) server.push(queue(request, QueueState.Admitted));
    });
    const client = await peer.connect();
    await client.admitWithCancellation(11n, 22n, "runner", 4500, new AbortController().signal);
    await client.logger.error("runner admitted");
    assertLog(peer.requests.at(-1)!, LogLevel.Error, "runner admitted");
    client.close();
  });
}

test("TypeScript decodes the Rust client-log golden", () => {
  const frame = readFileSync(new URL("../../woven-protocol/tests/fixtures/client_log_v1.swp", import.meta.url));
  assertLog(codec.decode(frame), LogLevel.Warn, "🧶 session warning");
});
