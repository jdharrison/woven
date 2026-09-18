import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import * as flatbuffers from "flatbuffers";
import { EnvelopeCodec } from "../src/codec.js";
import {
  Envelope, MessageKind, DeliveryClass, ControlPayload, QueueState,
  QueueUpdatePayload, RequestAdmissionPayload, QueueStatusRequestPayload,
  QueueHeartbeatPayload, QueueClaimPayload, QueueCancelPayload,
} from "../generated/woven/protocol/v1.js";

const codec = new EnvelopeCodec();

test("Rust managed golden decodes with exact u64 ticket and advisory lifetimes", () => {
  const frame = readFileSync(new URL("../../woven-protocol/tests/fixtures/queue_update_v1.swp", import.meta.url));
  const e = codec.decode(frame);
  assert.equal(e.messageKind, MessageKind.QueueUpdate);
  assert.equal(e.namespaceId, 1n);
  assert.equal(e.sessionId, 2n);
  assert.equal(e.correlationId, 3n);
  assert.ok(e.control instanceof QueueUpdatePayload);
  assert.equal(e.control.ticketId(), 18446744073709551615n);
  assert.equal(e.control.state(), QueueState.Offered);
  assert.equal(e.control.position(), 0);
  assert.equal(e.control.pollAfterMs(), 1000);
  assert.equal(e.control.ticketRemainingMs(), 120000);
  assert.equal(e.control.offerRemainingMs(), 30000);
});

function request(kind: MessageKind, invalid = false): Uint8Array {
  const b = new flatbuffers.Builder(256);
  const ticket = invalid ? 0n : 18446744073709551615n;
  let payload: number;
  switch (kind) {
    case MessageKind.RequestAdmission:
      payload = RequestAdmissionPayload.createRequestAdmissionPayload(b, b.createString(invalid ? "" : "request-1")); break;
    case MessageKind.QueueStatusRequest:
      payload = QueueStatusRequestPayload.createQueueStatusRequestPayload(b, ticket); break;
    case MessageKind.QueueHeartbeat:
      payload = QueueHeartbeatPayload.createQueueHeartbeatPayload(b, ticket); break;
    case MessageKind.QueueClaim:
      payload = QueueClaimPayload.createQueueClaimPayload(b, ticket); break;
    case MessageKind.QueueCancel:
      payload = QueueCancelPayload.createQueueCancelPayload(b, ticket); break;
    default: throw new Error("not a request");
  }
  const root = Envelope.createEnvelope(b, 1, kind, DeliveryClass.ReliableOrdered,
    1n, 2n, 0n, 0n, 0n, 0n, 0n, 3n, 0n, 0, (kind - 3) as ControlPayload, payload, 0n);
  Envelope.finishSizePrefixedEnvelopeBuffer(b, root);
  return b.asUint8Array();
}

test("all generated managed requests parse and reject absent keys/tickets", () => {
  for (const kind of [MessageKind.RequestAdmission, MessageKind.QueueStatusRequest,
    MessageKind.QueueHeartbeat, MessageKind.QueueClaim, MessageKind.QueueCancel]) {
    assert.equal(codec.decode(request(kind)).messageKind, kind);
    assert.throws(() => codec.decode(request(kind, true)), /invalid managed admission/);
  }
});
