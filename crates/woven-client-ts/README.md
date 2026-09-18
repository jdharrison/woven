# Woven TypeScript client (WebTransport)

The browser client for Woven, generated FlatBuffers bindings plus a real
`WebTransport` transport client that mirrors the native Rust `woven-client`
API. Per ADR 0014, browsers connect over **WebTransport** (QUIC over HTTPS/HTTP-3),
so this is the full native client path for web runtimes — not just a codec.

## Managed admission

The WebTransport client supports the managed WVN1 admission flow with the same bounds
and fail-closed behavior as the Rust client:

- `requestAdmission`, `queueStatus`, `queueHeartbeat`, `queueClaim`, and `queueCancel`
  perform one request/reply exchange with caller-supplied nonzero correlation IDs;
- `admitWithCancellation` starts at correlation ID 1, polls with monotone IDs, clamps
  server polling advice to 1–5 seconds, and never retries transport operations;
- each exchange is limited to 10 seconds and the caller's total timeout must be positive
  and no greater than 15 minutes;
- cancellation, timeout, malformed current replies, or ticket mismatches close the
  WebTransport session because partial stream I/O cannot safely be replayed;
- replies are dispatched by exact namespace/session/correlation; bounded stale replies remain
  available to `recv()`, and a `ProtocolError` is current only when its scope, correlation, and
  related request kind all match the active exchange;
- admission and queue replies are returned as normalized `AdmissionResult` and
  `QueueUpdate` objects. Tickets remain `bigint`; polling advice and remaining lifetimes
  are numbers in milliseconds. Zero remaining lifetime means unavailable, not a fresh TTL.

Use a fresh authenticated client before subscriptions or publishing. During a managed
exchange or runner, that operation exclusively owns the control-stream reader. A semantic
`Rejected`, `Paused`, `Cancelled`, `Expired`, or `Missing` response is returned as data,
not retried or converted into a transport error.

A Host-provided endpoint still must be an actual browser WebTransport endpoint. The
client does not convert a native-only managed QUIC listener into WebTransport support.

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
npm install @signalweave/woven-client@^0.2.0
```

```ts
import { AuthenticationScheme, WovenClient } from "@signalweave/woven-client";

const client = await WovenClient.connect({
  url: "quic://host:4433",
  token: "<bearer-token>",
  authenticationScheme: AuthenticationScheme.Bearer,
});

await client.joinSession(1n, 1n);
await client.subscribeSpace(1n, 1n, 1n, 1n, 1n);

const envelope = await client.recv();
if (envelope.messageKind === MessageKind.ReliableEvent) {
  console.log("got event:", new TextDecoder().decode(envelope.payload));
}
```

The client requires a runtime that implements the WHATWG `WebTransport` API (any
modern browser, or a Node shim). `authenticationScheme` defaults to
`AuthenticationScheme.Development` for compatibility with local development nodes;
remote managed deployments can explicitly select `AuthenticationScheme.Bearer`.
Bearer credentials are opaque server/Host-issued values, not a client-side JWT contract.
`connectTimeoutMs` defaults to 10 seconds and is one total monotonic deadline covering
WebTransport readiness, bidirectional stream creation, and the complete WVN1 handshake.
`maxFrameBytes` and `maxPayloadBytes` default to 64 KiB, must be positive bounded integers with
payload no larger than frame, and are enforced on incoming traffic. Frame limits are checked from
the four-byte prefix before body accumulation; partial input is capped at one frame and the
decoded control-stream inbox is capped at 64 envelopes. Any framing, payload, or inbox-bound
violation closes the connection.

Safe WHATWG constructor options can be supplied through `webTransportOptions`. At most eight
SHA-256 certificate hashes are accepted; each is validated as exactly 32 bytes and defensively
copied before the constructor is called:

```ts
const client = await WovenClient.connect({
  url: "https://127.0.0.1:4434/webtransport",
  token: "dev-token",
  webTransportOptions: {
    serverCertificateHashes: [{ algorithm: "sha-256", value: certificateHash }],
  },
});
```

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
- `requestAdmission(namespaceId, sessionId, correlationId, idempotencyKey)`
- `queueStatus(...)` / `queueHeartbeat(...)` / `queueClaim(...)` / `queueCancel(...)`
- `admitWithCancellation(namespaceId, sessionId, idempotencyKey, timeoutMs, signal)`
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

A bounded managed admission flow looks like this:

```ts
const cancellation = new AbortController();
const outcome = await client.admitWithCancellation(
  1n,
  1n,
  crypto.randomUUID(),
  60_000,
  cancellation.signal,
);

if (outcome.kind === "admission") {
  console.log(outcome.result.status);
} else {
  console.log(outcome.update.state);
}
```

Calling `cancellation.abort()` closes the connection. Create a fresh client before
attempting another admission operation.

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
