import { strict as assert } from "node:assert";
import { describe, test } from "node:test";
import * as flatbuffers from "flatbuffers";
import {
  AdmissionRejectionCode,
  AdmissionResultPayload,
  AdmissionStatus,
  AuthenticatePayload,
  AuthenticationScheme,
  ControlPayload,
  DeliveryClass,
  Envelope as FbEnvelope,
  MessageKind,
  QueueState,
  QueueUpdatePayload,
} from "../generated/woven/protocol/v1.js";
import { WovenClient, WovenError } from "../src/client.js";
import { DecodedEnvelope, EnvelopeCodec } from "../src/codec.js";
import {
  encodeAuthenticate,
  encodeReliableEvent,
  encodeQueueCancel,
  encodeQueueClaim,
  encodeQueueHeartbeat,
  encodeQueueStatusRequest,
  encodeRequestAdmission,
} from "../src/encode.js";
import { WebTransport, WebTransportBidirectionalStream } from "../src/webtransport.js";
import {
  buildAuthenticated,
  buildCapabilities,
  buildProtocolError,
} from "./wire-helpers.js";

const codec = new EnvelopeCodec();
const encoder = new TextEncoder();
const managedScope = { namespaceId: 11n, sessionId: 22n, correlationId: 33n };


function buildAdmissionResult(
  request: DecodedEnvelope,
  options: {
    status: AdmissionStatus;
    rejectionCode?: AdmissionRejectionCode;
    ticketId?: bigint;
    pollAfterMs?: number;
    ticketRemainingMs?: number;
    namespaceId?: bigint;
    correlationId?: bigint;
  },
): Uint8Array {
  const builder = new flatbuffers.Builder(256);
  const payload = AdmissionResultPayload.createAdmissionResultPayload(
    builder,
    options.status,
    options.rejectionCode ?? AdmissionRejectionCode.None,
    options.ticketId ?? 0n,
    options.pollAfterMs ?? 0,
    options.ticketRemainingMs ?? 0,
  );
  const root = FbEnvelope.createEnvelope(
    builder,
    1,
    MessageKind.AdmissionResult,
    DeliveryClass.ReliableOrdered,
    options.namespaceId ?? request.namespaceId,
    request.sessionId,
    0n,
    0n,
    0n,
    0n,
    0n,
    options.correlationId ?? request.correlationId ?? 0n,
    0n,
    0,
    ControlPayload.AdmissionResultPayload,
    payload,
    0n,
  );
  FbEnvelope.finishSizePrefixedEnvelopeBuffer(builder, root);
  return builder.asUint8Array();
}

function buildQueueUpdate(
  request: DecodedEnvelope,
  options: {
    ticketId: bigint;
    state: QueueState;
    position?: number;
    pollAfterMs?: number;
    ticketRemainingMs?: number;
    offerRemainingMs?: number;
    sessionId?: bigint;
    correlationId?: bigint;
  },
): Uint8Array {
  const builder = new flatbuffers.Builder(256);
  const payload = QueueUpdatePayload.createQueueUpdatePayload(
    builder,
    options.ticketId,
    options.state,
    options.position ?? 0,
    options.pollAfterMs ?? 0,
    options.ticketRemainingMs ?? 0,
    options.offerRemainingMs ?? 0,
  );
  const root = FbEnvelope.createEnvelope(
    builder,
    1,
    MessageKind.QueueUpdate,
    DeliveryClass.ReliableOrdered,
    request.namespaceId,
    options.sessionId ?? request.sessionId,
    0n,
    0n,
    0n,
    0n,
    0n,
    options.correlationId ?? request.correlationId ?? 0n,
    0n,
    0,
    ControlPayload.QueueUpdatePayload,
    payload,
    0n,
  );
  FbEnvelope.finishSizePrefixedEnvelopeBuffer(builder, root);
  return builder.asUint8Array();
}

type RequestHandler = (request: DecodedEnvelope, server: ManagedFakeServer) => void;

class ManagedFakeServer {
  readonly requests: DecodedEnvelope[] = [];
  readonly bidi: WebTransportBidirectionalStream;
  closeCount = 0;
  closeReason: string | undefined;
  private readonly handler: RequestHandler;
  private readonly controller: ReadableStreamDefaultController<Uint8Array>;
  private acc = new Uint8Array(0);
  private streamClosed = false;

  constructor(handler: RequestHandler = () => {}) {
    this.handler = handler;
    let controller!: ReadableStreamDefaultController<Uint8Array>;
    const readable = new ReadableStream<Uint8Array>({
      start(value) {
        controller = value;
      },
    });
    this.controller = controller;
    const writable = new WritableStream<Uint8Array>({
      write: (chunk) => this.ingest(new Uint8Array(chunk)),
    });
    this.bidi = { readable, writable };
  }

  push(frame: Uint8Array): void {
    if (!this.streamClosed) this.controller.enqueue(frame);
  }

  close(reason?: string): void {
    this.closeCount += 1;
    this.closeReason = reason;
    if (!this.streamClosed) {
      this.streamClosed = true;
      this.controller.close();
    }
  }

  private ingest(chunk: Uint8Array): void {
    const merged = new Uint8Array(this.acc.length + chunk.length);
    merged.set(this.acc);
    merged.set(chunk, this.acc.length);
    this.acc = merged;
    for (;;) {
      const result = codec.decodeStream(this.acc);
      if (result === null) return;
      this.acc = this.acc.subarray(result.consumed);
      const request = result.envelope;
      this.requests.push(request);
      if (request.messageKind === MessageKind.Hello) {
        this.push(buildCapabilities());
      } else if (request.messageKind === MessageKind.Authenticate) {
        this.push(buildAuthenticated());
      } else {
        this.handler(request, this);
      }
    }
  }
}

function makeTransport(server: ManagedFakeServer): WebTransport {
  return {
    ready: Promise.resolve(),
    closed: new Promise(() => {}),
    datagrams: {
      readable: new ReadableStream(),
      writable: new WritableStream(),
      incomingMaxAge: null,
      outgoingMaxAge: null,
      incomingHighWaterMark: 0,
      outgoingHighWaterMark: 0,
    },
    createBidirectionalStream: async () => server.bidi,
    close: (info) => server.close(info?.reason),
  };
}

async function connect(
  server: ManagedFakeServer,
  authenticationScheme = AuthenticationScheme.Development,
): Promise<WovenClient> {
  return WovenClient.fromTransport(makeTransport(server), server.bidi, {
    url: "https://localhost:4433/webtransport",
    token: "credential",
    authenticationScheme,
  });
}

function hasMessage(error: unknown, fragment: string): boolean {
  return (error as WovenError).message.includes(fragment);
}

describe("managed outbound encoding and authentication", () => {
  test("Authenticate preserves Development default and supports explicit Bearer", () => {
    const development = codec.decode(encodeAuthenticate(encoder.encode("dev")));
    const bearer = codec.decode(
      encodeAuthenticate(encoder.encode("opaque"), AuthenticationScheme.Bearer),
    );
    assert.ok(development.control instanceof AuthenticatePayload);
    assert.ok(bearer.control instanceof AuthenticatePayload);
    assert.equal(development.control.scheme(), AuthenticationScheme.Development);
    assert.equal(bearer.control.scheme(), AuthenticationScheme.Bearer);
    assert.throws(
      () => encodeAuthenticate(encoder.encode("bad"), AuthenticationScheme.Unknown),
      /authentication scheme/,
    );
  });

  test("all managed encoders carry exact scope, correlation, and ticket", () => {
    const frames = [
      encodeRequestAdmission(managedScope, "request-1"),
      encodeQueueStatusRequest(managedScope, 44n),
      encodeQueueHeartbeat(managedScope, 44n),
      encodeQueueClaim(managedScope, 44n),
      encodeQueueCancel(managedScope, 44n),
    ];
    const kinds = [
      MessageKind.RequestAdmission,
      MessageKind.QueueStatusRequest,
      MessageKind.QueueHeartbeat,
      MessageKind.QueueClaim,
      MessageKind.QueueCancel,
    ];
    frames.forEach((frame, index) => {
      const envelope = codec.decode(frame);
      assert.equal(envelope.messageKind, kinds[index]);
      assert.equal(envelope.namespaceId, 11n);
      assert.equal(envelope.sessionId, 22n);
      assert.equal(envelope.correlationId, 33n);
    });
  });

  test("managed encoders reject zero correlation, ticket, and invalid keys", () => {
    assert.throws(
      () => encodeRequestAdmission({ ...managedScope, correlationId: 0n }, "request-1"),
      /correlation ID/,
    );
    assert.throws(() => encodeQueueClaim(managedScope, 0n), /ticket ID/);
    assert.throws(() => encodeRequestAdmission(managedScope, ""), /idempotency key/);
    assert.throws(() => encodeRequestAdmission(managedScope, "x".repeat(257)), /idempotency key/);
  });

  test("handshake sends explicit Bearer authentication", async () => {
    const server = new ManagedFakeServer();
    const client = await connect(server, AuthenticationScheme.Bearer);
    const authenticate = server.requests[1]!.control;
    assert.ok(authenticate instanceof AuthenticatePayload);
    assert.equal(authenticate.scheme(), AuthenticationScheme.Bearer);
    assert.deepEqual(authenticate.credentialsArray(), encoder.encode("credential"));
    client.close();
  });
});

describe("low-level managed admission methods", () => {
  test("return normalized admission and queue results", async () => {
    const ticketId = 9_007_199_254_740_993n;
    const server = new ManagedFakeServer((request, peer) => {
      if (request.messageKind === MessageKind.RequestAdmission) {
        peer.push(
          buildAdmissionResult(request, {
            status: AdmissionStatus.Queued,
            ticketId,
            pollAfterMs: 1_500,
            ticketRemainingMs: 120_000,
          }),
        );
      } else {
        const states: Partial<Record<MessageKind, QueueState>> = {
          [MessageKind.QueueStatusRequest]: QueueState.Waiting,
          [MessageKind.QueueHeartbeat]: QueueState.Offered,
          [MessageKind.QueueClaim]: QueueState.Admitted,
          [MessageKind.QueueCancel]: QueueState.Cancelled,
        };
        const state = states[request.messageKind]!;
        peer.push(
          buildQueueUpdate(request, {
            ticketId,
            state,
            position: state === QueueState.Waiting ? 3 : 0,
            pollAfterMs:
              state === QueueState.Waiting || state === QueueState.Offered ? 1_000 : 0,
            ticketRemainingMs:
              state === QueueState.Waiting || state === QueueState.Offered ? 100_000 : 0,
            offerRemainingMs: state === QueueState.Offered ? 20_000 : 0,
          }),
        );
      }
    });
    const client = await connect(server);

    assert.deepEqual(await client.requestAdmission(1n, 2n, 10n, "request-1"), {
      status: AdmissionStatus.Queued,
      rejectionCode: AdmissionRejectionCode.None,
      ticketId,
      pollAfterMs: 1_500,
      ticketRemainingMs: 120_000,
    });
    assert.equal((await client.queueStatus(1n, 2n, 11n, ticketId)).state, QueueState.Waiting);
    assert.equal(
      (await client.queueHeartbeat(1n, 2n, 12n, ticketId)).state,
      QueueState.Offered,
    );
    assert.equal((await client.queueClaim(1n, 2n, 13n, ticketId)).state, QueueState.Admitted);
    assert.equal((await client.queueCancel(1n, 2n, 14n, ticketId)).state, QueueState.Cancelled);
    client.close();
  });

  test("keeps a stale non-admission frame queued while dispatching the current reply", async () => {
    const server = new ManagedFakeServer((request, peer) => {
      peer.push(
        encodeReliableEvent(
          {
            namespaceId: 1n,
            sessionId: 2n,
            spaceId: 3n,
            spaceEpoch: 1n,
            channelId: 4n,
            entityId: 5n,
            senderSequence: 1n,
          },
          { typeId: 1n, bytes: encoder.encode("stale") },
        ),
      );
      peer.push(buildAdmissionResult(request, { status: AdmissionStatus.Admitted }));
    });
    const client = await connect(server);
    const result = await client.requestAdmission(1n, 2n, 7n, "request-1");
    assert.equal(result.status, AdmissionStatus.Admitted);
    const stale = await client.recv();
    assert.equal(stale.messageKind, MessageKind.ReliableEvent);
    assert.equal(stale.spaceId, 3n);
    assert.equal(server.closeCount, 0);
    client.close();
  });

  test("dispatches a stale wrong-correlation reply without treating it as current", async () => {
    const server = new ManagedFakeServer((request, peer) => {
      peer.push(
        buildAdmissionResult(request, {
          status: AdmissionStatus.Admitted,
          correlationId: request.correlationId! - 1n,
        }),
      );
      peer.push(buildAdmissionResult(request, { status: AdmissionStatus.Admitted }));
    });
    const client = await connect(server);
    const result = await client.requestAdmission(1n, 2n, 7n, "request-1");
    assert.equal(result.status, AdmissionStatus.Admitted);
    const stale = await client.recv();
    assert.equal(stale.messageKind, MessageKind.AdmissionResult);
    assert.equal(stale.correlationId, 6n);
    assert.equal(server.closeCount, 0);
    client.close();
  });

  test("does not treat wrong-scope or wrong-correlation ProtocolErrors as current", async () => {
    const server = new ManagedFakeServer((request, peer) => {
      peer.push(
        buildProtocolError({
          relatedKind: MessageKind.RequestAdmission,
          namespaceId: request.namespaceId,
          sessionId: request.sessionId,
          correlationId: request.correlationId! + 1n,
        }),
      );
      peer.push(
        buildProtocolError({
          relatedKind: MessageKind.RequestAdmission,
          namespaceId: request.namespaceId,
          sessionId: request.sessionId + 1n,
          correlationId: request.correlationId!,
        }),
      );
      peer.push(buildAdmissionResult(request, { status: AdmissionStatus.Admitted }));
    });
    const client = await connect(server);
    const result = await client.requestAdmission(1n, 2n, 7n, "request-1");
    assert.equal(result.status, AdmissionStatus.Admitted);
    const wrongCorrelation = await client.recv();
    const wrongScope = await client.recv();
    assert.equal(wrongCorrelation.messageKind, MessageKind.ProtocolError);
    assert.equal(wrongCorrelation.correlationId, 8n);
    assert.equal(wrongScope.messageKind, MessageKind.ProtocolError);
    assert.equal(wrongScope.sessionId, 3n);
    assert.equal(server.closeCount, 0);
    client.close();
  });

  test("closes when a current ProtocolError names the wrong related message kind", async () => {
    const server = new ManagedFakeServer((request, peer) => {
      peer.push(
        buildProtocolError({
          relatedKind: MessageKind.QueueClaim,
          namespaceId: request.namespaceId,
          sessionId: request.sessionId,
          correlationId: request.correlationId!,
        }),
      );
    });
    const client = await connect(server);
    await assert.rejects(
      client.requestAdmission(1n, 2n, 7n, "request-1"),
      (error) => hasMessage(error, "does not match the active admission operation"),
    );
    assert.equal(server.closeCount, 1);
    assert.equal(server.closeReason, "admission stopped");
  });

  test("closes on queue ticket mismatch", async () => {
    const server = new ManagedFakeServer((request, peer) => {
      peer.push(
        buildQueueUpdate(request, {
          ticketId: 99n,
          state: QueueState.Missing,
        }),
      );
    });
    const client = await connect(server);
    await assert.rejects(
      client.queueStatus(1n, 2n, 7n, 88n),
      (error) => hasMessage(error, "ticket mismatch"),
    );
    assert.equal(server.closeCount, 1);
  });

  test("enforces an exclusive control-stream reader", async () => {
    const server = new ManagedFakeServer();
    const client = await connect(server);
    const receive = client.recv();
    await assert.rejects(
      client.requestAdmission(1n, 2n, 1n, "request-1"),
      (error) => hasMessage(error, "active stream"),
    );
    client.close();
    await assert.rejects(receive, (error) => hasMessage(error, "control stream ended"));
  });

  test("closes when a low-level exchange exceeds ten seconds", async (context) => {
    const server = new ManagedFakeServer();
    const client = await connect(server);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    try {
      const admission = client.requestAdmission(1n, 2n, 1n, "request-1");
      await Promise.resolve();
      context.mock.timers.tick(10_000);
      await assert.rejects(admission, (error) => hasMessage(error, "operation timed out"));
      assert.equal(server.closeCount, 1);
    } finally {
      context.mock.timers.reset();
    }
  });
});

describe("admitWithCancellation", () => {
  test("polls no faster than one second, then claims an offer with monotone correlations", async () => {
    const ticketId = 77n;
    const operationTimes: number[] = [];
    const started = Date.now();
    const server = new ManagedFakeServer((request, peer) => {
      if (request.messageKind === MessageKind.RequestAdmission) {
        peer.push(
          buildAdmissionResult(request, {
            status: AdmissionStatus.Queued,
            ticketId,
            pollAfterMs: 0,
            ticketRemainingMs: 120_000,
          }),
        );
      } else if (request.messageKind === MessageKind.QueueHeartbeat) {
        operationTimes.push(Date.now() - started);
        peer.push(
          buildQueueUpdate(request, {
            ticketId,
            state: QueueState.Offered,
            pollAfterMs: 0,
            ticketRemainingMs: 100_000,
            offerRemainingMs: 30_000,
          }),
        );
      } else if (request.messageKind === MessageKind.QueueClaim) {
        operationTimes.push(Date.now() - started);
        peer.push(buildQueueUpdate(request, { ticketId, state: QueueState.Admitted }));
      }
    });
    const client = await connect(server);
    const outcome = await client.admitWithCancellation(
      1n,
      2n,
      "request-1",
      10_000,
      new AbortController().signal,
    );

    assert.deepEqual(outcome, {
      kind: "queue",
      update: {
        ticketId,
        state: QueueState.Admitted,
        position: 0,
        pollAfterMs: 0,
        ticketRemainingMs: 0,
        offerRemainingMs: 0,
      },
    });
    assert.ok(operationTimes[0]! >= 900);
    assert.ok(operationTimes[1]! >= 1_900);
    const managed = server.requests.slice(2);
    assert.deepEqual(
      managed.map((request) => request.messageKind),
      [MessageKind.RequestAdmission, MessageKind.QueueHeartbeat, MessageKind.QueueClaim],
    );
    assert.deepEqual(
      managed.map((request) => request.correlationId),
      [1n, 2n, 3n],
    );
    assert.equal(server.closeCount, 0);
    client.close();
  });

  test("clamps long polling advice to five seconds", async (context) => {
    const ticketId = 78n;
    const server = new ManagedFakeServer((request, peer) => {
      if (request.messageKind === MessageKind.RequestAdmission) {
        peer.push(
          buildAdmissionResult(request, {
            status: AdmissionStatus.Queued,
            ticketId,
            pollAfterMs: 30_000,
            ticketRemainingMs: 120_000,
          }),
        );
      } else {
        peer.push(buildQueueUpdate(request, { ticketId, state: QueueState.Missing }));
      }
    });
    const client = await connect(server);
    context.mock.timers.enable({ apis: ["setTimeout"] });
    try {
      const admission = client.admitWithCancellation(
        1n,
        2n,
        "request-1",
        60_000,
        new AbortController().signal,
      );
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(server.requests.length, 3);
      context.mock.timers.tick(4_999);
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.equal(server.requests.length, 3);
      context.mock.timers.tick(1);
      const outcome = await admission;
      assert.equal(outcome.kind, "queue");
      if (outcome.kind === "queue") assert.equal(outcome.update.state, QueueState.Missing);
      assert.equal(server.requests[3]?.messageKind, MessageKind.QueueHeartbeat);
      client.close();
    } finally {
      context.mock.timers.reset();
    }
  });

  test("returns immediate semantic admission without retry", async () => {
    const server = new ManagedFakeServer((request, peer) => {
      peer.push(
        buildAdmissionResult(request, {
          status: AdmissionStatus.Rejected,
          rejectionCode: AdmissionRejectionCode.QueueFull,
        }),
      );
    });
    const client = await connect(server);
    const outcome = await client.admitWithCancellation(
      1n,
      2n,
      "request-1",
      5_000,
      new AbortController().signal,
    );
    assert.equal(outcome.kind, "admission");
    if (outcome.kind === "admission") {
      assert.equal(outcome.result.status, AdmissionStatus.Rejected);
      assert.equal(outcome.result.rejectionCode, AdmissionRejectionCode.QueueFull);
    }
    assert.equal(server.requests.length, 3);
    client.close();
  });

  test("cancellation during polling closes the transport", async () => {
    const cancellation = new AbortController();
    const server = new ManagedFakeServer((request, peer) => {
      peer.push(
        buildAdmissionResult(request, {
          status: AdmissionStatus.Queued,
          ticketId: 55n,
          pollAfterMs: 5_000,
          ticketRemainingMs: 120_000,
        }),
      );
      queueMicrotask(() => cancellation.abort());
    });
    const client = await connect(server);
    await assert.rejects(
      client.admitWithCancellation(1n, 2n, "request-1", 10_000, cancellation.signal),
      (error) => hasMessage(error, "cancelled"),
    );
    assert.equal(server.closeCount, 1);
    assert.equal(server.requests.length, 3);
  });

  test("total deadline is bounded and closes the transport", async () => {
    const server = new ManagedFakeServer((request, peer) => {
      peer.push(
        buildAdmissionResult(request, {
          status: AdmissionStatus.Queued,
          ticketId: 66n,
          pollAfterMs: 5_000,
          ticketRemainingMs: 120_000,
        }),
      );
    });
    const client = await connect(server);
    await assert.rejects(
      client.admitWithCancellation(
        1n,
        2n,
        "request-1",
        20,
        new AbortController().signal,
      ),
      (error) => hasMessage(error, "deadline exceeded"),
    );
    assert.equal(server.closeCount, 1);
  });

  test("rejects invalid total bounds before starting admission", async () => {
    const server = new ManagedFakeServer();
    const client = await connect(server);
    await assert.rejects(
      client.admitWithCancellation(
        1n,
        2n,
        "request-1",
        15 * 60 * 1_000 + 1,
        new AbortController().signal,
      ),
      (error) => hasMessage(error, "at most 15 minutes"),
    );
    assert.equal(server.closeCount, 0);
    assert.equal(server.requests.length, 2);
    client.close();
  });
});
