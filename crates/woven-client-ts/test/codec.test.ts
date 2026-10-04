import { test, describe } from "node:test";
import { strict as assert } from "node:assert";
import { readFileSync } from "node:fs";
import * as flatbuffers from "flatbuffers";
import {
  encodeHello,
  encodeAuthenticate,
  encodeJoinSession,
  encodeSubscribeSpace,
  encodeReliableEvent,
  encodeEntityState,
  encodeSnapshotRequest,
  encodeSpaceTransition,
  encodeInferenceRequested,
} from "../src/encode.js";
import {
  EnvelopeCodec,
  CodecError,
  DEFAULT_MAX_FRAME_BYTES,
  DEFAULT_MAX_PAYLOAD_BYTES,
  PROTOCOL_VERSION,
} from "../src/codec.js";
import {
  ControlPayload,
  DeliveryClass,
  Envelope as FbEnvelope,
  HelloPayload,
  MessageKind,
} from "../generated/woven/protocol/v1.js";
import { buildCapabilities, createEnvelope } from "./wire-helpers.js";

const codec = new EnvelopeCodec();
const encoder = new TextEncoder();

describe("EnvelopeCodec decodes Rust-produced wire frames", () => {
  const fixture = readFileSync(
    "../woven-protocol/tests/fixtures/reliable_event_v1.swp",
  );

  test("reliable_event_v1.swp (written by the Rust Codec)", () => {
    const envelope = codec.decode(new Uint8Array(fixture));
    assert.equal(envelope.protocolVersion, PROTOCOL_VERSION);
    assert.equal(envelope.messageKind, MessageKind.ReliableEvent);
    assert.equal(envelope.deliveryClass, DeliveryClass.ReliableOrdered);
    assert.equal(envelope.controlType, 0);
    assert.equal(envelope.control, null);
    assert.ok(envelope.channelId !== null && envelope.channelId > 0n);
    assert.ok(envelope.entityId !== null && envelope.entityId > 0n);
    assert.ok(envelope.payload !== null && envelope.payload.length > 0);
  });
});

describe("TypeScript encoder produces Rust-decodable, semantically correct frames", () => {
  test("hello", () => {
    const envelope = codec.decode(
      encodeHello({ clientName: "woven-client-ts", clientVersion: "0.1.0" }),
    );
    assert.equal(envelope.messageKind, MessageKind.Hello);
    assert.equal(envelope.deliveryClass, DeliveryClass.ReliableOrdered);
    assert.equal(envelope.namespaceId, 0n);
    assert.equal(envelope.sessionId, 0n);
  });

  test("authenticate", () => {
    const envelope = codec.decode(encodeAuthenticate(encoder.encode("dev-token")));
    assert.equal(envelope.messageKind, MessageKind.Authenticate);
    assert.equal(envelope.deliveryClass, DeliveryClass.ReliableOrdered);
  });

  test("join-session", () => {
    const envelope = codec.decode(
      encodeJoinSession({ namespaceId: 1n, sessionId: 1n }),
    );
    assert.equal(envelope.messageKind, MessageKind.JoinSession);
    assert.equal(envelope.namespaceId, 1n);
    assert.equal(envelope.sessionId, 1n);
  });

  test("subscribe-space", () => {
    const envelope = codec.decode(
      encodeSubscribeSpace({
        namespaceId: 1n,
        sessionId: 1n,
        spaceId: 1n,
        spaceEpoch: 1n,
        channelId: 1n,
      }),
    );
    assert.equal(envelope.messageKind, MessageKind.SubscribeSpace);
    assert.equal(envelope.spaceId, 1n);
    assert.equal(envelope.spaceEpoch, 1n);
    assert.equal(envelope.channelId, 1n);
  });

  test("reliable-event carries payload, ids and sequence", () => {
    const envelope = codec.decode(
      encodeReliableEvent(
        {
          namespaceId: 1n,
          sessionId: 1n,
          spaceId: 1n,
          spaceEpoch: 1n,
          channelId: 1n,
          entityId: 1n,
          senderSequence: 1n,
        },
        { typeId: 1n, bytes: encoder.encode("hi") },
      ),
    );
    assert.equal(envelope.messageKind, MessageKind.ReliableEvent);
    assert.equal(envelope.namespaceId, 1n);
    assert.equal(envelope.spaceId, 1n);
    assert.equal(envelope.entityId, 1n);
    assert.equal(envelope.channelId, 1n);
    assert.equal(envelope.senderSequence, 1n);
    assert.equal(envelope.payloadTypeId, 1n);
    assert.deepEqual(envelope.payload, encoder.encode("hi"));
  });

  test("entity-state uses LatestValue delivery", () => {
    const envelope = codec.decode(
      encodeEntityState(
        {
          namespaceId: 1n,
          sessionId: 1n,
          spaceId: 1n,
          spaceEpoch: 1n,
          channelId: 2n,
          entityId: 7n,
          senderSequence: 3n,
        },
        { typeId: 5n, bytes: encoder.encode("state") },
      ),
    );
    assert.equal(envelope.messageKind, MessageKind.EntityState);
    assert.equal(envelope.deliveryClass, DeliveryClass.LatestValue);
    assert.equal(envelope.entityId, 7n);
    assert.equal(envelope.senderSequence, 3n);
    assert.equal(envelope.payloadTypeId, 5n);
  });

  test("entity-state round-trips an additive finite 3D routing position", () => {
    const envelope = codec.decode(
      encodeEntityState(
        {
          namespaceId: 1n,
          sessionId: 1n,
          spaceId: 3n,
          spaceEpoch: 1n,
          channelId: 1n,
          entityId: 7n,
          senderSequence: 4n,
          routingPosition: { x: -1.25, y: 2.5, z: 100 },
        },
        { typeId: 5n, bytes: encoder.encode("positioned") },
      ),
    );
    assert.deepEqual(envelope.routingPosition, { x: -1.25, y: 2.5, z: 100 });
    assert.deepEqual(envelope.payload, encoder.encode("positioned"));
  });
});

describe("Default payload bounds are checked before FlatBuffers serialization", () => {
  const limit = 65_536;
  const scope = {
    namespaceId: 1n, sessionId: 1n, spaceId: 1n, spaceEpoch: 1n, channelId: 1n, entityId: 1n,
  };
  const encoders = [
    { name: "entity-state", encode: (length: number) => encodeEntityState(scope, { typeId: 1n, bytes: new Uint8Array(length) }) },
    { name: "reliable-event", encode: (length: number) => encodeReliableEvent(scope, { typeId: 1n, bytes: new Uint8Array(length) }) },
    { name: "authenticate", encode: (length: number) => encodeAuthenticate(new Uint8Array(length)) },
    { name: "join-session resume token", encode: (length: number) => encodeJoinSession({ namespaceId: scope.namespaceId, sessionId: scope.sessionId }, new Uint8Array(length)) },
    {
      name: "hello combined UTF-8 strings",
      encode: (length: number) => encodeHello({ clientName: "é".repeat(limit / 2), clientVersion: "x".repeat(length - limit) }),
    },
    {
      name: "inference capability and input",
      encode: (length: number) => encodeInferenceRequested(scope, { capability: "é", deadlineMs: 500n, input: new Uint8Array(length - 2) }),
    },
  ];

  for (const { name, encode } of encoders) {
    test(`${name} accepts 65536 bytes and rejects 65537 bytes before serialization`, (t) => {
      const frame = encode(limit);
      assert.ok(frame.byteLength > limit);
      assert.doesNotThrow(() => codec.decode(frame));
      const vectors = t.mock.method(flatbuffers.Builder.prototype, "createByteVector", () => {
        assert.fail("oversized payload reached FlatBuffers byte-vector serialization");
      });
      const strings = t.mock.method(flatbuffers.Builder.prototype, "createString", () => {
        assert.fail("oversized payload reached FlatBuffers string serialization");
      });
      assert.throws(
        () => encode(limit + 1),
        (error: unknown) => error instanceof CodecError &&
          error.code === "PayloadTooLarge" &&
          error.message === "payload length 65537 bytes exceeds limit 65536 bytes",
      );
      assert.equal(vectors.mock.calls.length, 0);
      assert.equal(strings.mock.calls.length, 0);
    });
  }

  test("codec and Hello defaults leave room for payload plus framing overhead", () => {
    assert.equal(DEFAULT_MAX_PAYLOAD_BYTES, limit);
    assert.equal(DEFAULT_MAX_FRAME_BYTES, 1_048_576);
    assert.equal(codec.maxPayloadBytes, limit);
    assert.equal(codec.maxFrameBytes, 1_048_576);
    const hello = codec.decode(encodeHello({})).control as HelloPayload;
    assert.equal(hello.maxPayloadSize(), limit);
    assert.equal(hello.maxFrameSize(), 1_048_576);
  });

  test("generic codec still permits explicit payload limits above the default", () => {
    const builder = new flatbuffers.Builder(1024);
    const payload = builder.createByteVector(new Uint8Array(limit + 1));
    const root = createEnvelope(
      builder,
      PROTOCOL_VERSION,
      MessageKind.ReliableEvent,
      DeliveryClass.ReliableOrdered,
      1n, 1n, 1n, 1n, 1n,
      0n, 0n, 0n,
      1n,
      payload,
      ControlPayload.NONE,
      0,
      1n,
    );
    FbEnvelope.finishSizePrefixedEnvelopeBuffer(builder, root);
    const frame = builder.asUint8Array();
    assert.throws(
      () => codec.decode(frame),
      (error: unknown) => error instanceof CodecError && error.code === "PayloadTooLarge",
    );
    const explicit = new EnvelopeCodec(DEFAULT_MAX_FRAME_BYTES, limit + 1);
    assert.equal(explicit.decode(frame).payload?.byteLength, limit + 1);
  });
});

describe("EnvelopeCodec stream framing", () => {
  test("decodeStream reassembles frames across byte chunks", () => {
    const bytes = encodeHello({});
    let acc = new Uint8Array(0);
    let consumedTotal = 0;
    let envelope: ReturnType<EnvelopeCodec["decode"]> | undefined;
    for (let i = 0; i < bytes.length; i++) {
      const next = new Uint8Array(acc.length + 1);
      next.set(acc);
      next[acc.length] = bytes[i]!;
      acc = next;
      const result = codec.decodeStream(acc);
      if (result) {
        envelope = result.envelope;
        consumedTotal += result.consumed;
        acc = acc.subarray(result.consumed);
      }
    }
    assert.ok(envelope, "decoded an envelope");
    assert.equal(envelope.messageKind, MessageKind.Hello);
    assert.equal(consumedTotal, bytes.length);
    assert.equal(acc.length, 0);
  });

  test("decodeStream returns null until a full frame is buffered", () => {
    const bytes = encodeHello({});
    const partial = bytes.subarray(0, 4);
    assert.equal(codec.decodeStream(partial), null);
  });

  test("decodeStream rejects an oversized frame from the four-byte prefix", () => {
    const bounded = new EnvelopeCodec(128, 64);
    const prefix = new Uint8Array(4);
    new DataView(prefix.buffer).setUint32(0, 128, true);
    assert.throws(
      () => bounded.decodeStream(prefix),
      (error: unknown) => error instanceof CodecError && error.code === "FrameTooLarge",
    );
  });
});

describe("EnvelopeCodec error handling", () => {
  test("rejects a truncated frame", () => {
    const bytes = encodeHello({});
    assert.throws(() => codec.decode(bytes.subarray(0, 10)), CodecError);
  });

  test("rejects a frame with a mismatched size prefix", () => {
    const bytes = encodeHello({});
    const wrong = bytes.slice();
    wrong[0] = 0xff;
    assert.throws(() => codec.decode(wrong), CodecError);
  });

  test("rejects a frame with a wrong file identifier", () => {
    const bytes = encodeHello({});
    const wrong = bytes.slice();
    wrong[10] = 0x58; // corrupt an identifier byte
    assert.throws(() => codec.decode(wrong), CodecError);
  });

  test("rejects opaque payloads above the configured payload limit", () => {
    const frame = encodeReliableEvent(
      {
        namespaceId: 1n,
        sessionId: 1n,
        spaceId: 1n,
        spaceEpoch: 1n,
        channelId: 1n,
        entityId: 1n,
      },
      { typeId: 1n, bytes: new Uint8Array(3) },
    );
    assert.throws(
      () => new EnvelopeCodec(1024, 2).decode(frame),
      (error: unknown) => error instanceof CodecError && error.code === "PayloadTooLarge",
    );
  });

  test("rejects variable-length control payloads above the configured payload limit", () => {
    assert.throws(
      () => new EnvelopeCodec(1024, 4).decode(buildCapabilities()),
      (error: unknown) => error instanceof CodecError && error.code === "PayloadTooLarge",
    );
  });

  test("rejects invalid frame and payload limit combinations", () => {
    assert.throws(() => new EnvelopeCodec(0, 1), /invalid codec limits/);
    assert.throws(() => new EnvelopeCodec(64, 65), /invalid codec limits/);
  });
});

describe("Extended control encoders round-trip", () => {
  test("snapshot-request", () => {
    const envelope = codec.decode(
      encodeSnapshotRequest(
        { namespaceId: 1n, sessionId: 1n, spaceId: 1n, spaceEpoch: 1n, channelId: 1n },
        42n,
      ),
    );
    assert.equal(envelope.messageKind, MessageKind.SnapshotRequest);
    assert.equal(envelope.channelId, 1n);
  });

  test("space-transition", () => {
    const envelope = codec.decode(
      encodeSpaceTransition(
        {
          namespaceId: 1n,
          sessionId: 1n,
          spaceId: 1n,
          spaceEpoch: 1n,
          entityId: 3n,
        },
        { fromSpaceId: 1n, toSpaceId: 2n, toSpaceEpoch: 1n },
      ),
    );
    assert.equal(envelope.messageKind, MessageKind.SpaceTransition);
    assert.equal(envelope.entityId, 3n);
    assert.equal(envelope.spaceId, 1n);
  });

  test("inference-requested", () => {
    const envelope = codec.decode(
      encodeInferenceRequested(
        {
          namespaceId: 1n,
          sessionId: 1n,
          spaceId: 1n,
          spaceEpoch: 1n,
          entityId: 9n,
        },
        { capability: "language.dialogue", deadlineMs: 500n, input: encoder.encode("q") },
      ),
    );
    assert.equal(envelope.messageKind, MessageKind.InferenceRequested);
    assert.equal(envelope.entityId, 9n);
  });
});
