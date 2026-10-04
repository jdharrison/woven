# Woven TypeScript client (WebTransport)

The browser client for Woven, generated FlatBuffers bindings plus a real
`WebTransport` transport client that mirrors the native Rust `woven-client`
API. Per ADR 0014, browsers connect over **WebTransport** (QUIC over HTTPS/HTTP-3),
so this is the full native client path for web runtimes — not just a codec.

## Session logger

After joining an unmanaged session or receiving a verified managed `Admitted`
result, use the remembered session without scope arguments:

```ts
await client.logger.info("connected");
await client.logger.warn("cache miss");
await client.logger.error("operation failed");
await client.log("info alias");
```

Each method is `(message: string): Promise<void>`. `ClientLogger` is exported.
The logger sends one session-scoped `ClientLog` over the ordered control stream;
it is not broadcast to other clients. **Resolution means sent, not server
acceptance, Host persistence, or a persistence acknowledgement.** Continue
receiving control traffic to observe server rejections; do not send secrets.

Messages must contain 1–1024 UTF-8 bytes (`MAX_LOG_MESSAGE_BYTES`), not characters;
smaller negotiated payload/frame limits apply. No joined/admitted session,
invalid messages, and concurrent writes reject locally, without retries or a
logging queue. Logger sends preserve the client's existing admission/write
exclusivity. Successful request admission, queue operations returning Admitted
(including claim), and `admitWithCancellation` automatically remember scope.
Queued/Paused/Rejected results do not establish it.

Legacy join has no success acknowledgement: scope is remembered after sending
`joinSession` and invalidated by an observed join rejection. `leaveSession(reason?)`
leaves the remembered session and clears scope; close, transport closure/I/O
failure, and observed invalid-session log errors also clear it. Subsequent logs
require another join/admission. `CAPABILITY_CLIENT_LOG = 2n`, `LogLevel`,
`ClientLogPayload`, and `encodeClientLog(scope, level, message)` are exported for
protocol tooling. If the server did not negotiate `CAPABILITY_CLIENT_LOG`, all
logger methods and `log` reject locally with `{ kind: "protocol", message:
"server did not negotiate ClientLog" }` before writing bytes. Joined scope is
retained and the connection remains usable.

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

After the `0.3.0` candidate has been published (see the workspace release checklist):

```sh
npm install @signalweave/woven-client@^0.3.0
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
`maxFrameBytes` defaults to 1 MiB (1,048,576 bytes), leaving room for envelope/framing overhead;
`maxPayloadBytes` defaults to 64 KiB (65,536 bytes). Both must be positive bounded integers with
payload no larger than frame, and are enforced on incoming traffic. Frame limits are checked from
the four-byte prefix before body accumulation; partial input is capped at one frame and the
decoded control-stream inbox is capped at 64 envelopes. Any incoming control-stream framing,
payload, or inbox-bound violation closes the connection.

### Positioned entity state

The client advertises `CAPABILITY_POSITIONED_ENTITY_STATE = 1n` in `Hello` and stores the
capability intersection returned by the server. Check `client.supportsPositionedState()` before
using either positioned API; both reject locally with `kind: "protocol"` and perform no write if
the capability was not negotiated.

```ts
await client.publishPositionedState(
  namespaceId, sessionId, spaceId, spaceEpoch, latestValueChannelId,
  entityId, sequence, typeId, { x: 9.9, y: 0, z: 0 }, stateBytes,
);
await client.publishUnreliablePositionedState(
  namespaceId, sessionId, spaceId, spaceEpoch, unreliableChannelId,
  entityId, sequence, typeId, { x: 9.9, y: 0, z: 0 }, stateBytes,
);
```

The first method sends `EntityState/LatestValue` on the reliable stream; the second sends
`EntityState/UnreliableSequenced` on the datagram lane. Position is finite 3D routing metadata
alongside opaque payload bytes. The server validates the target 3D space and inclusive bounds and
applies position plus state atomically. Client resolution means transport write completion, not
server acceptance or peer delivery.

Server channel policy remains authoritative. Current managed spatial spaces expose channel 1 as
`ReliableOrdered` events and channel 4 as `UnreliableSequenced` state, so managed positioned state
uses the unreliable method. `publishPositionedState` applies to compositions with a matching
`LatestValue` channel and does not override channel 1.

### Independent unreliable datagram lane

`publishUnreliableState(namespaceId, sessionId, spaceId, spaceEpoch, channelId, entityId,
sequence, typeId, payload): Promise<void>` queues exactly one size-prefixed WVN1 opaque
`EntityState` frame with hardcoded `DeliveryClass.UnreliableSequenced`. Its nine arguments
match `publishState`; it writes only `transport.datagrams.writable`. The channel must already
be provisioned by the server with the corresponding delivery/persistence policy; the client
cannot override that policy. Payloads are domain-agnostic bytes, not a built-in transform/pose format.

Use `webTransportOptions: { requireUnreliable: true }` when datagrams are required.
A missing datagram API, nonpositive/unavailable `datagrams.maxDatagramSize`, or a reported
`reliable-only` transport rejects lane operations with the existing `{ kind, message }` error
shape. Embedded `fromTransport` sessions also enforce `requireUnreliable` during connection.
There is **no fragmentation, retry, or reliable fallback**. A resolved publish promise means
only that the runtime accepted the write, not that a peer received it.

After the early publishing payload check, the **complete encoded frame**, including its
four-byte prefix and FlatBuffers metadata, must fit both configured `maxFrameBytes` and the
server's advertised frame limit, and the transport's **current** `datagrams.maxDatagramSize`.
Frame-limit rejections are `kind: "protocol"`; MTU rejections are `kind: "transport"`.
Neither local rejection writes a packet or closes the connection. The practical payload budget
is usually much smaller than 64 KiB because of the transport MTU.

`recvDatagram(): Promise<DecodedEnvelope | null>` blocks on the datagram lane only.
`recvDatagramTimeout(ms): Promise<DecodedEnvelope | null>` returns `null` on timeout; both
return `null` on lane end or when an already-pending read is stopped by closure. New calls
on a locally closed client reject with `kind: "closed"`. `recv()` / `recvTimeout()` remain
control-stream-only and can run concurrently with datagram I/O.

There is at most one active receive caller, one datagram reader, and one datagram writer,
independent of the control-stream reader/writer. A timeout retains at most one pending
receive promise (possibly one resolved packet) for the next datagram receive. One read observer
and one replaceable waiter keep repeated timeout polling bounded even if no packet ever arrives.
It never starts a competing read or a background decoded inbox. Closing settles pending lane calls, cancels
the reader, and aborts the writer without waiting indefinitely for runtime cancellation.
Managed admission/queue exchanges and runners reject datagram use; admission cannot start
while a lane read, retained receive, or write is outstanding.

Each packet is decoded independently with the configured codec frame/payload limits. The
lane accepts only opaque `EntityState` / `UnreliableSequenced` envelopes with nonzero
namespace/session/space/epoch/channel/entity/type IDs. Sender sequences are u64s and may start
at zero, matching the Rust client and WVN1; subsequent updates must increase monotonically for
the same server-side sequence scope. A malformed packet
is discarded and that receive rejects with `kind: "protocol"`; **only one packet is consumed
per call**, so there is no unbounded malformed-packet skip loop. The reliable control lane
remains usable. Loss, duplication, and out-of-order arrival are possible: the client validates
sequence values but does not provide ordering or maintain a per-entity sequence map.

The client sets WHATWG datagram queue high-water marks to **8 incoming / 1 outgoing** and
fails connection if an available runtime cannot apply those bounds. Optional
`datagramMaxAgeMs` sets both incoming/outgoing queue maximum ages (positive finite milliseconds,
at most 2,147,483,647); the default is `null` (no age expiry), not a domain-specific cadence.
These are runtime queue settings, not guaranteed peer-delivery counts. Run a separate bounded
receive loop for datagrams:

```ts
const client = await WovenClient.connect({
  url: "https://localhost:4434/webtransport",
  token: "dev-token",
  webTransportOptions: { requireUnreliable: true },
  datagramMaxAgeMs: 250, // optional application-selected queue expiry
});

await client.publishUnreliableState(1n, 1n, 1n, 1n, 4n, entityId, sequence, typeId, bytes);
const packet = await client.recvDatagramTimeout(100);
// Independently consume subscription/lifecycle/error envelopes with client.recv().
```

### Publishing payload limits

`publishState`, `publishPositionedState`, `publishEvent`, `publishUnreliableState`, and
`publishUnreliablePositionedState` reject payloads larger than the smallest of **65,536 bytes**,
configured `maxPayloadBytes`, and the server's advertised payload limit. A larger configured or
server limit does not raise the publishing ceiling. Exactly 65,536 payload bytes are supported
with the default frame limit; envelope metadata and framing overhead are separate from payload
bytes. Payload rejection happens before FlatBuffers serialization or transport writes, reports
the actual byte length and effective limit, and leaves the connection usable. All reliable writes
also check the complete encoded frame against the smaller configured/server frame limit before
transport I/O, including positioned state and controls; an oversized frame rejects locally with
`kind: "protocol"` without closing an established connection.

The limit applies to the **full serialized payload of each update**, not to each individual
property or to character count. For example, JSON counts as its UTF-8 encoded bytes. A channel's
server-controlled payload cap may be lower still; the server enforces that cap, and the client
cannot override it. Send granular entity/property deltas rather than a whole-world state blob.

The exported encode helpers also enforce the default 64 KiB ceiling before FlatBuffers
serialization, including the combined UTF-8 string and byte-vector content of variable-size
controls. The low-level `EnvelopeCodec` defaults to the same payload ceiling but continues to
accept explicit frame/payload limits for decoding; this does not raise the publishing ceiling.

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
- `publishState(...)` (LatestValue/entity state on the reliable stream)
- `supportsPositionedState()` / `publishPositionedState(..., position, payload)`
- `publishUnreliableState(namespaceId, sessionId, spaceId, spaceEpoch, channelId, entityId, sequence, typeId, payload): Promise<void>`
- `publishUnreliablePositionedState(..., position, payload): Promise<void>`
- `requestInference(...)`
- `recv()` / `recvTimeout(ms)` (control stream, unchanged)
- `recvDatagram()` / `recvDatagramTimeout(ms)` (independent unreliable entity-state lane; `Promise<DecodedEnvelope | null>`)
- `close(closeCode?, reason?)` / `closeGracefully(timeoutMs?, closeCode?, reason?)`

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
npm run test:browser # real managed Woven + headless Chromium/WebTransport smoke
```

The browser test requires `cargo`, `openssl`, and Playwright's Chromium installation
(`npx playwright install chromium`). It creates disposable loopback credentials and TLS files,
provisions one managed scope, and verifies connect, Bearer authentication, admission,
one space-wide subscription on channel 1, and the fixture's channel list `[1, 4]`.
With `requireUnreliable: true`, it publishes an opaque **25-byte sample pose fixture** on
channel 4 / type 1 with sequences starting at 1, then validates an actual
`EntityState/UnreliableSequenced` datagram echo's scope, sequence, and exact bytes via
`recvDatagramTimeout(5_000)`. The bytes are test data, not a Woven server domain codec.
Normal datagram loss is accommodated with new monotonic updates at at most 10 Hz for up to
2 seconds / 20 attempts; only a matching received echo counts as success, never a resolved
write. Results report attempted sends, echoed sequence, and zero reliable control-stream
writes during the pose phase (observed through the public stream API).
A separate reliable channel-1 event must still echo afterward, followed by graceful
disconnect and CCU release. All targets are loopback; this is not deployed/hosted validation.
Longer runs remain bounded:

```sh
npm run test:browser -- --iterations=300
npm run test:browser -- --duration-seconds=900
```

## Regenerating bindings

```sh
cargo build -p woven-protocol
FLATC=$(find target/debug/build -path '*/out/bin/flatc' -type f -print -quit)
"$FLATC" --ts -o crates/woven-client-ts/generated crates/woven-protocol/schemas/woven_v1.fbs
```

Regenerating produces only a diff when the protocol schema changes; commit it
alongside the schema change and the corresponding Rust bindings.
