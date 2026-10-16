import assert from "node:assert/strict";
import test from "node:test";
import * as flatbuffers from "flatbuffers";
import {
  AdmissionStatus, DeliveryClass, MessageKind, EnvelopeCodec, AuthenticationScheme,
  type DecodedEnvelope,
} from "../src/index.js";
import {
  AdmissionResultPayload, AdmissionRejectionCode, AuthenticatePayload, ControlPayload,
  Envelope, SubscriptionAcceptedPayload, EntityEnteredPayload,
} from "../generated/woven/protocol/v1.js";
import {
  fetchBootstrap, probeConnection, validateTarget, positiveId, canonicalBootstrapPath,
  MAX_STARTUP_ENVELOPES, type Target, type ProbeConnection,
} from "../scripts/remote-browser-probe-page.js";
import { buildAuthenticated, buildCapabilities, createEnvelope } from "./wire-helpers.js";

const target: Target = {
  version: 1, url: "https://api.woven.host:4434/webtransport",
  namespaceId: "11", sessionId: "22", spaceId: "3", spaceEpoch: "1",
};
const syntheticToken = "a".repeat(64);
const kinds = [MessageKind.Hello, MessageKind.Authenticate, MessageKind.RequestAdmission, MessageKind.SubscribeSpace];
function entityPayload(entityId = 9n): EntityEnteredPayload {
  const builder = new flatbuffers.Builder(64);
  builder.finish(EntityEnteredPayload.createEntityEnteredPayload(builder, entityId));
  return EntityEnteredPayload.getRootAsEntityEnteredPayload(new flatbuffers.ByteBuffer(builder.asUint8Array()));
}
function envelope(kind: MessageKind, overrides: Partial<DecodedEnvelope> = {}): DecodedEnvelope {
  return {
    messageKind: kind, namespaceId: 11n, sessionId: 22n, spaceId: 3n, spaceEpoch: 1n,
    channelId: kind === MessageKind.SubscriptionAccepted ? 1n : null,
    entityId: kind === MessageKind.EntityEntered ? 9n : null,
    deliveryClass: DeliveryClass.ReliableOrdered,
    controlType: kind === MessageKind.EntityEntered ? ControlPayload.EntityEnteredPayload : ControlPayload.SubscriptionAcceptedPayload,
    control: kind === MessageKind.SubscriptionAccepted ? { subscriptionId: () => 8n, acceptedSpaceEpoch: () => 1n } :
      kind === MessageKind.EntityEntered ? entityPayload() : null,
    ...overrides,
  } as DecodedEnvelope;
}
const accepted = () => envelope(MessageKind.SubscriptionAccepted);
const entered = () => envelope(MessageKind.EntityEntered);
function fake(envelopes = [accepted(), entered()], status = AdmissionStatus.Admitted) {
  let calls = 0, closed = 0, stopped = 0, subscriptions = 0;
  const connection: ProbeConnection = {
    outboundKinds: kinds,
    client: {
      async requestAdmission(ns, session, correlation, key) {
        calls += 1; assert.equal(ns, 11n); assert.equal(session, 22n); assert.equal(correlation, 1n);
        assert.match(key, /^[0-9a-f-]{36}$/);
        return { status, rejectionCode: AdmissionRejectionCode.None, ticketId: null, pollAfterMs: 0, ticketRemainingMs: 0 };
      },
      async subscribeSpace(...scope) { subscriptions += 1; assert.deepEqual(scope, [11n, 22n, 3n, 1n, 1n]); },
      async recvTimeout() { return envelopes.shift() ?? accepted(); },
      async closeGracefully(timeout) { closed += 1; assert.equal(timeout, 2_000); },
    },
    stop() { stopped += 1; },
  };
  return { connection, counts: () => ({ calls, closed, stopped, subscriptions }) };
}
async function run(fixture: ReturnType<typeof fake>, signal = new AbortController().signal) {
  return probeConnection(target, syntheticToken, "1", signal, async () => fixture.connection);
}

test("admission plus matching acknowledgement and entity is the only success; closes once", async () => {
  const f = fake(); const result = await run(f);
  assert.equal(result.ok, true); assert.equal(result.entityId, "9"); assert.equal(result.close, "closed");
  assert.equal(result.authenticated, true); assert.equal(result.admitted, true); assert.equal(result.subscriptionAccepted, true);
  assert.deepEqual(f.counts(), { calls: 1, closed: 1, stopped: 1, subscriptions: 1 });
  assert.equal(JSON.stringify(result).includes(syntheticToken), false);
});

for (const status of [AdmissionStatus.Queued, AdmissionStatus.Rejected]) {
  test(`non-admitted status ${status} never subscribes or retries`, async () => {
    const f = fake([], status); const result = await run(f);
    assert.equal(result.ok, false); assert.equal(result.admitted, false);
    assert.deepEqual(f.counts(), { calls: 1, closed: 1, stopped: 1, subscriptions: 0 });
    if (status === AdmissionStatus.Queued) assert.equal(result.code, "admission-queued-not-success");
  });
}
for (const [name, frames] of [
  ["namespace", [envelope(MessageKind.SubscriptionAccepted, { namespaceId: 99n })]],
  ["session", [envelope(MessageKind.SubscriptionAccepted, { sessionId: 99n })]],
  ["space", [envelope(MessageKind.SubscriptionAccepted, { spaceId: 99n })]],
  ["epoch", [envelope(MessageKind.SubscriptionAccepted, { spaceEpoch: 99n })]],
  ["channel", [envelope(MessageKind.SubscriptionAccepted, { channelId: 4n })]],
  ["ack payload", [envelope(MessageKind.SubscriptionAccepted, { control: null })]],
  ["entity scope", [accepted(), envelope(MessageKind.EntityEntered, { spaceId: 99n })]],
  ["missing entity", [accepted(), envelope(MessageKind.EntityEntered, { entityId: null })]],
  ["zero entity", [accepted(), envelope(MessageKind.EntityEntered, { entityId: 0n })]],
  ["early entity", [entered(), accepted()]],
  ["rejection", [envelope(MessageKind.SubscriptionRejected)]],
  ["protocol error", [envelope(MessageKind.ProtocolError)]],
] as const) {
  test(`startup rejects ${name} without treating a write as acknowledgement`, async () => {
    const f = fake([...frames]); const result = await run(f);
    assert.equal(result.ok, false); assert.equal(result.entityId, null); assert.equal(result.close, "closed");
  });
}

for (const [name, overrides] of [
  ["missing payload", { control: null }],
  ["wrong control discriminator", { controlType: ControlPayload.SubscriptionAcceptedPayload }],
  ["wrong payload type", { control: new SubscriptionAcceptedPayload() }],
  ["zero payload entity ID", { control: entityPayload(0n) }],
  ["mismatched payload entity ID", { control: entityPayload(10n) }],
] as const) {
  test(`EntityEntered rejects ${name} and still closes`, async () => {
    const result = await run(fake([accepted(), envelope(MessageKind.EntityEntered, overrides)]));
    assert.equal(result.ok, false); assert.equal(result.code, "entity-assignment-invalid");
    assert.equal(result.entityId, null); assert.equal(result.close, "closed");
  });
}

test("startup is envelope bounded even when every frame is a valid acknowledgement", async () => {
  const result = await run(fake([accepted()]));
  assert.equal(result.ok, false); assert.equal(result.startupEnvelopes, MAX_STARTUP_ENVELOPES);
  assert.equal(result.code, "startup-bound-exceeded");
});

test("cancellation closes pending admission, does not expose thrown messages", async () => {
  const f = fake(); const controller = new AbortController();
  f.connection.client.requestAdmission = () => { controller.abort(); return new Promise(() => {}); };
  const result = await run(f, controller.signal);
  assert.equal(result.ok, false); assert.equal(result.close, "closed");
  assert.equal(result.code, "cancelled-or-overall-deadline");
  const failing = fake();
  failing.connection.client.requestAdmission = async () => { throw new Error(syntheticToken); };
  assert.equal(JSON.stringify(await run(failing)).includes(syntheticToken), false);
});

test("a stalled startup is cancellable and its pending receive cannot become success", async () => {
  const f = fake(); const controller = new AbortController();
  f.connection.client.recvTimeout = () => new Promise(() => {});
  const timer = setTimeout(() => controller.abort(), 10);
  try {
    const result = await run(f, controller.signal);
    assert.equal(result.ok, false); assert.equal(result.close, "closed");
    assert.equal(result.code, "cancelled-or-overall-deadline");
  } finally { clearTimeout(timer); }
});

test("a hanging transport close hits the two-second teardown bound", { timeout: 3_000 }, async () => {
  const f = fake(); const started = performance.now();
  f.connection.client.closeGracefully = () => new Promise(() => {});
  const result = await run(f);
  assert.equal(result.ok, false); assert.equal(result.close, "failed"); assert.equal(result.code, "teardown-failed");
  assert.ok(performance.now() - started < 2_500); assert.equal(f.counts().stopped, 1);
});

test("teardown failure cannot produce a successful result", async () => {
  const f = fake(); f.connection.client.closeGracefully = async () => { throw new Error(syntheticToken); };
  const result = await run(f);
  assert.equal(result.ok, false); assert.equal(result.code, "teardown-failed"); assert.equal(result.close, "failed");
  assert.equal(JSON.stringify(result).includes(syntheticToken), false);
});

function response(fields: Record<string, unknown> = {}, headers: Record<string, string> = {}) {
  return new Response(JSON.stringify({ ...target, token: syntheticToken, ...fields }), {
    headers: { "Content-Type": "application/json", "Cache-Control": "no-store, private", ...headers },
  });
}
test("bootstrap is one bounded, cookie-free request; target and cache policy are enforced", async () => {
  let requests = 0;
  const fetcher = (async (url, init) => {
    requests += 1; assert.equal(url, "https://jdharrison.com/api/lobby");
    assert.equal(init?.credentials, "omit"); assert.equal(init?.cache, "no-store");
    assert.equal(init?.redirect, "error"); assert.equal(init?.referrerPolicy, "no-referrer");
    return response();
  }) as typeof fetch;
  assert.equal(await fetchBootstrap("/api/lobby", target, "https://jdharrison.com", new AbortController().signal, fetcher), syntheticToken);
  assert.equal(requests, 1);
  for (const bad of [response({ sessionId: "23" }), response({ ttl: 30 }), response({}, { "Cache-Control": "public" }), response({}, { "Content-Type": "text/html" })]) {
    await assert.rejects(fetchBootstrap("/api/lobby", target, "https://jdharrison.com", new AbortController().signal, async () => bad));
  }
  await assert.rejects(fetchBootstrap("/api/lobby", target, "https://jdharrison.com", new AbortController().signal,
    async () => new Response("x".repeat(8_193), { headers: { "Content-Type": "application/json", "Cache-Control": "no-store, private" } })), /bootstrap-too-large/);
});

for (const cache of ["no-store", "private, no-store"]) {
  test(`bootstrap accepts Cache-Control: ${cache}`, async () => {
    const credential = await fetchBootstrap("/api/lobby", target, "https://jdharrison.com", new AbortController().signal,
      async () => response({}, { "Cache-Control": cache }));
    assert.equal(credential === syntheticToken, true);
  });
}

for (const cache of [null, "private", "no-cache", "max-age=0"]) {
  test(`bootstrap rejects ${cache === null ? "missing Cache-Control" : `Cache-Control: ${cache}`} without no-store`, async () => {
    const reply = response({}, { "Cache-Control": cache ?? "" });
    if (cache === null) reply.headers.delete("Cache-Control");
    await assert.rejects(fetchBootstrap("/api/lobby", target, "https://jdharrison.com", new AbortController().signal,
      async () => reply), /bootstrap-cache-policy-unsafe/);
  });
}

test("descriptors reject unsafe URLs, imprecise IDs, extra fields and noncanonical routes", () => {
  assert.deepEqual(validateTarget({ ...target }, target.url), target);
  for (const value of ["0", "01", "18446744073709551616", 1]) assert.throws(() => positiveId(value));
  for (const path of ["https://other.test/", "//other.test", "/api/../lobby", "/api?token=redacted", ""]) {
    assert.throws(() => canonicalBootstrapPath(path, "https://jdharrison.com"));
  }
  assert.throws(() => validateTarget({ ...target, token: syntheticToken }, target.url));
  assert.throws(() => validateTarget({ ...target, url: "https://other.test/webtransport" }, target.url));
});

function controlFrame(kind: MessageKind, request: DecodedEnvelope): Uint8Array {
  const b = new flatbuffers.Builder(256);
  let payload: number, type: ControlPayload;
  if (kind === MessageKind.AdmissionResult) {
    type = ControlPayload.AdmissionResultPayload;
    payload = AdmissionResultPayload.createAdmissionResultPayload(b, AdmissionStatus.Admitted, AdmissionRejectionCode.None, 0n, 0, 0);
  } else if (kind === MessageKind.SubscriptionAccepted) {
    type = ControlPayload.SubscriptionAcceptedPayload; payload = SubscriptionAcceptedPayload.createSubscriptionAcceptedPayload(b, 8n, request.spaceEpoch);
  } else {
    type = ControlPayload.EntityEnteredPayload; payload = EntityEnteredPayload.createEntityEnteredPayload(b, 9n);
  }
  const admission = kind === MessageKind.AdmissionResult;
  const root = createEnvelope(b, 1, kind, DeliveryClass.ReliableOrdered, request.namespaceId, request.sessionId,
    admission ? 0n : request.spaceId, kind === MessageKind.EntityEntered ? 9n : 0n, admission ? 0n : request.spaceEpoch,
    0n, 0n, admission ? request.correlationId! : 0n, 0n, 0, type, payload,
    kind === MessageKind.SubscriptionAccepted ? request.channelId! : 0n);
  Envelope.finishSizePrefixedEnvelopeBuffer(b, root); return b.asUint8Array();
}

test("real SDK over a mock transport sends only Hello, Bearer Authenticate, RequestAdmission, SubscribeSpace", async (t) => {
  const codec = new EnvelopeCodec(); let controller!: ReadableStreamDefaultController<Uint8Array>;
  let completeClose!: (value: object) => void; let instances = 0;
  const seen: number[] = [];
  class Transport {
    ready = Promise.resolve(); closed = new Promise<object>((resolve) => { completeClose = resolve; });
    datagrams = { maxDatagramSize: 1200, readable: new ReadableStream(), writable: new WritableStream() };
    constructor(url: string, options: object) {
      instances += 1; assert.equal(url, target.url); assert.deepEqual(options, { requireUnreliable: true });
    }
    async createBidirectionalStream() {
      return {
        readable: new ReadableStream<Uint8Array>({ start(value) { controller = value; } }),
        writable: new WritableStream<Uint8Array>({ write(bytes) {
          const request = codec.decode(bytes); seen.push(request.messageKind);
          if (request.messageKind === MessageKind.Hello) controller.enqueue(buildCapabilities());
          if (request.messageKind === MessageKind.Authenticate) {
            assert.equal((request.control as AuthenticatePayload).scheme(), AuthenticationScheme.Bearer);
            controller.enqueue(buildAuthenticated());
          }
          if (request.messageKind === MessageKind.RequestAdmission) controller.enqueue(controlFrame(MessageKind.AdmissionResult, request));
          if (request.messageKind === MessageKind.SubscribeSpace) {
            controller.enqueue(controlFrame(MessageKind.SubscriptionAccepted, request));
            controller.enqueue(controlFrame(MessageKind.EntityEntered, request));
          }
        } }),
      };
    }
    close() { completeClose({}); }
  }
  const previous = Object.getOwnPropertyDescriptor(globalThis, "WebTransport");
  Object.defineProperty(globalThis, "WebTransport", { value: Transport, configurable: true });
  t.after(() => { if (previous) Object.defineProperty(globalThis, "WebTransport", previous); else Reflect.deleteProperty(globalThis, "WebTransport"); });
  const result = await probeConnection(target, syntheticToken, "1", new AbortController().signal);
  assert.equal(result.ok, true, JSON.stringify(result)); assert.equal(instances, 1);
  assert.deepEqual(seen, kinds); assert.deepEqual(result.outboundKinds, kinds);
});
