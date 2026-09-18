# Woven TypeScript client (WebTransport)

The browser client for Woven, generated FlatBuffers bindings plus a real
`WebTransport` transport client that mirrors the native Rust `woven-client`
API. Per ADR 0014, browsers connect over **WebTransport** (QUIC over HTTPS/HTTP-3),
so this is the full native client path for web runtimes — not just a codec.

## Managed admission compatibility

Generated WVN1 bindings include RequestAdmission, AdmissionResult, QueueStatusRequest,
QueueHeartbeat, QueueClaim, QueueCancel, and QueueUpdate. `EnvelopeCodec` parses them
and validates managed scope, correlation, union/kind matching, IDs and result fields.
Tickets remain `bigint`; polling advice and remaining lifetimes are numbers in ms.
Zero remaining lifetime means unavailable, not a fresh TTL. Public exports include
managed payload classes and AdmissionStatus/AdmissionRejectionCode/QueueState enums.

This is **wire codec compatibility only**: no managed browser WebTransport composition,
queue runner, or remote managed browser support is provided. Do not infer a managed
WebTransport endpoint from a Host-provided native QUIC endpoint. The Rust native
client owns this slice's managed connection/admission API.

## What's here

- `generated/` — flatc `--ts` output. Regenerate after any schema change (see below).
- `src/` — the transport client and framing codec.
  - `src/client.ts` — `WovenClient`: connect, join, subscribe, publish, receive.
  - `src/codec.ts` — `EnvelopeCodec`: size-prefixed FlatBuffers framing, decode-only.
  - `src/encode.ts` — builds outbound envelopes for every client message kind.
  - `src/webtransport.ts` — minimal WHATWG `WebTransport` interface types.
  - `src/index.ts` — public entry point.
- `test/` — tests with Node's built-in test runner:
  - `codec.test.ts` — encode/decode and stream framing.
  - `client.test.ts` — client behavior driven by an in-memory mock WebTransport.

The encoder is validated for wire compatibility in both directions:
- `test/codec.test.ts` decodes the checked-in Rust golden fixture.
- `crates/woven-protocol/tests/ts_client_wire.rs` decodes frames produced by
  this encoder with the Rust `Codec`, proving the server can consume TS output.

## Using the client (browser)

```sh
npm install @signalweave/woven-client
```

```ts
import { WovenClient } from "@signalweave/woven-client";

const client = await WovenClient.connect({
  url: "quic://host:4433",
  token: "<bearer-token>",
});

await client.joinSession(1n, 1n);
await client.subscribeSpace(1n, 1n, 1n, 1n, 1n);

const envelope = await client.recv();
if (envelope.messageKind === MessageKind.ReliableEvent) {
  console.log("got event:", new TextDecoder().decode(envelope.payload));
}
```

The client requires a runtime that implements the WHATWG `WebTransport` API (any
modern browser, or a Node shim).

## Standardized `quic://` URL and the deterministic port convention

The standardized Woven server URL is `quic://host:port` — a single scheme used by
every client regardless of runtime. Native clients use `quic://` directly as QUIC; a
browser client only speaks WebTransport, so it derives the WebTransport endpoint from the
`quic://` URL using the **deterministic port convention**: WebTransport listens one port
above the native QUIC port, on the `/webtransport` path.

| input | resolved WebTransport URL |
|---|---|
| `quic://relay.example:8081` | `https://relay.example:8082/webtransport` |
| `quic://relay.example` | `https://relay.example:4434/webtransport` (default port 4433) |
| `wtransport://h:p/webtransport` | `https://h:p/webtransport` (as-is) |
| `https://h:p/webtransport` | `https://h:p/webtransport` (as-is) |

An explicit `wtransport://…` or `https://…/webtransport` URL is used as-is, so the
convention is a default that deployments can override.

## API surface

`WovenClient` mirrors `woven-client`:

- `connect(config)` / `fromTransport(transport, stream, config)`
- `joinSession(namespaceId, sessionId)`
- `subscribeSpace(namespaceId, sessionId, spaceId, spaceEpoch, channelId)`
- `transitionEntity(...)`
- `requestSnapshot(...)`
- `publishEvent(namespaceId, sessionId, spaceId, spaceEpoch, channelId, entityId, sequence, typeId, payload)`
- `publishState(...)` (LatestValue/entity state)
- `requestInference(...)`
- `recv()` / `recvTimeout(ms)`
- `close(closeCode?, reason?)`

All IDs are `bigint`. `recv()` returns a normalized `DecodedEnvelope` (message kind,
delivery class, scoping IDs, payload, and the typed control payload when present).

## Running the tests

```sh
cd crates/woven-client-ts
npm ci
npm run format:check
npm run lint
npm run typecheck
npm test          # unit tests (codec + mocked WebTransport client)
npm run build
npm run test:decode-fixture
npm run test:decode-tool-call-completed
```

## Regenerating bindings

```sh
cargo build -p woven-protocol
FLATC=$(find target/debug/build -path '*/out/bin/flatc' -type f -print -quit)
"$FLATC" --ts -o crates/woven-client-ts/generated crates/woven-protocol/schemas/woven_v1.fbs
```

Regenerating produces only a diff when the protocol schema changes; commit it
alongside the schema change like the Rust, C#, and Python bindings.
