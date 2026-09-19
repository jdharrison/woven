# Woven Status

Woven is feature-complete for its own scope: a transport-neutral realtime core, a
versioned FlatBuffers wire protocol, two interchangeable realtime transports, spatial
interest routing with a load runner, and an optional adjacent inference plane. It is
designed to be self-hosted standalone, the way you'd self-host Redis or Postgres, with no
dependency on any hosted control plane or console.

## What's implemented

**Core** (`woven-core`) — validated typed IDs, explicit namespace/session/space/channel
grants, server-provisioned bounded sessions, nested anchored spaces with epoch tombstones,
entities and ownership, subscriptions, server-controlled delivery/persistence policy,
monotonic sequencing, bounded in-memory state with optional per-channel TTL (actively
swept, not just lazily hidden, once expired) behind a `CacheService` seam (ADR-0015) for
a future Redis/NoSQL backend, rate and payload limits, priority-aware
bounded/coalescing queues, stale-queue purging, immediate slow-consumer cleanup, a bounded
journal outbox with a no-op sink, a deterministic worker harness, and per-session
admission control: capacity allocation, FIFO queueing with offers, reconnect grace,
usage counters, and configurable windowed aggregation with in-memory/JSONL/spooling sinks.

**Protocol** (`woven-protocol`) — the full v1 metadata envelope and typed control
messages, including inference/tool-call lifecycle and managed admission/queue controls
(message kinds 33–39), a pinned vendored FlatBuffers
compiler, verifier-backed bounded decoding, semantic validation, and checked-in golden
fixtures proving byte-for-byte cross-language stability.

**Realtime transports** — an Axum control plane (`woven-server`) exposing
`/healthz`, `/readyz`, `/v1/capabilities`, and `/metrics` (Prometheus text: live
connection/session counts plus cumulative publish/delivery/byte/rejection/backpressure
counters — always-on, not gated behind debug builds); a bounded single-owner Tokio
worker and protocol bridge shared by every adapter (`woven-transport`); native QUIC
and (in the same `woven-transport-quic` crate) browser WebTransport, both mapping
unreliable/best-effort delivery to datagrams under a conservative packet budget. Binary
WebSocket was removed as a transport (ADR 0014): native clients speak QUIC, browsers speak
WebTransport, sharing the same envelope codec and delivery-class mapping. Real-socket
conformance coverage exists for both, and clients negotiate/observe available transports
through `/v1/capabilities`. The standardized server URI is `quic://host:port` for every
client; a browser maps it to WebTransport via the deterministic port convention
(WebTransport one port above QUIC, on `/webtransport`). The development HTTP control plane
also exposes admission and queue endpoints (`/v1/virtual-servers/{server_id}/join`,
`/v1/queues/{ticket}`, etc.) and an operational snapshot route.

**Opt-in remote native QUIC** — `woven-server` now supports explicit PEM certificate/key
and static scoped-token file configuration, with loopback-only management HTTP and no
remote WebTransport/inference. `woven-client::Client::connect_with_tls` verifies the
certificate chain, validity, and URL DNS/IP SAN against supplied CA roots. Defaults remain
local development; insecure development listeners/clients cannot use non-loopback targets.
This is a fixed explicitly provisioned namespace/session 1, spaces 1/2, channels 1/2
composition using the existing development authentication scheme, **not production tenant
identity or hosted auth**. Local real-QUIC tests cover trust/name rejection, wrong/default
tokens, authorization, fanout and disconnect. No cloud deployment or external target test
has been performed. See the [server configuration](../crates/woven-server/README.md) and
[exact client API](../crates/woven-client-rust/README.md).

**Opt-in managed QUIC/WebTransport runtime** — `ManagedServerConfig` / `start_managed`
starts empty, with mandatory native QUIC, optional WebTransport, separate read-only management,
and an authenticated loopback admin listener. Managed WebTransport has its own explicit UDP bind
address and request path; it does not infer a QUIC-plus-one endpoint. Its bounded allowlist accepts
only exact canonical browser HTTP(S) origins and rejects missing origins. Both data-plane
transports use the configured leaf-first PEM chain and require the WVN1 Bearer authentication
scheme; development/static compositions keep Development compatibility. Authenticated
`GET /v1/node` reports whether WebTransport is live and, when enabled, the lowercase SHA-256 DER
fingerprint of the actual configured leaf certificate, but no endpoint URL.

The Host-compatible node/session API implements scoped provision/read/capacity/delete,
incarnation binding, revision retries and bounded revocation history. SHA-256 session-token
verifiers and unique connection principals live in the core worker. Admission and joins are
atomic; deletion closes admitted, waiting and not-yet-joined sockets on both managed transports.
The WVN1 bridge and Rust client admission/queue APIs are tested over local TLS-verified QUIC,
including CCU-one queue/heartbeat/claim, duplicate operations, ticket ownership, scope isolation,
ordinary-join rejection, correlated rate errors, teardown and helper cancellation. A real
TLS-verified WebTransport test client covers the same worker bridge, direct and queued admission,
origin rejection, Bearer enforcement, scope deletion, fingerprint metadata, and shutdown.
Runtime tests also cover response fields, replay, limits and configuration.
The real Host-to-managed-node E2E in
`crates/woven-server/tests/host_managed_local.rs` is implemented and has passed via
`npm run test:local` from `../woven-host` (relative to Woven's root). It uses
real Host HTTP APIs, authenticated node admin HTTP, and Host-returned descriptors
with the TLS-verified native QUIC client, backed by isolated Firebase Auth/Firestore
emulators on loopback. Coverage includes ownership/capacity, monitoring, queue/claim,
TLS/scope rejection, server deletion and account teardown. This cross-repository test
is ignored by ordinary Cargo runs and must be launched through Host's local runner.

Managed mode uses opaque shared session credentials, fixed spaces/channels, zero reconnect grace,
and in-memory configuration/revocation history. Rust native QUIC and TypeScript WebTransport
clients expose bounded admission/queue APIs and cancellation helpers. Admission exchanges are
exclusive pre-subscription operations with ten-second timeouts; helper deadlines are positive,
capped at 15 minutes, and perform zero transport retries. Wire remaining-lifetime fields are
currently zero (unavailable), not fresh TTLs. The TypeScript admission implementation is covered
with a mock WHATWG transport, Rust/TypeScript wire compatibility, and a bounded real
headless-Chromium E2E that connects the public client API to a disposable managed Woven
WebTransport listener. It provisions one scope and verifies Bearer authentication, admission,
subscription, publish/echo,
graceful disconnect, and CCU release. CI and release validation install Playwright Chromium and
gate on one bounded iteration. Validation also includes the local Host API-to-native-QUIC-client
path and Rust-driven real WebTransport sockets, but not an application browser UI,
Host-provided WebTransport descriptors, Weaver integration, external deployment, production
Firebase/App Check, durable recovery/failover, or production per-user identity. See
[managed sessions](managed-sessions.md).

**Interest management** (`woven-core` + `woven-loadtest`) — bounded 2D/3D
spatial grid routing for replaceable state, with owner-updated positions, cell indexes,
radius filtering, optional exact distance checks, and reliable-event bypass; a bounded
local load runner for broadcast, topic, 2D-grid, and 3D-grid scenarios reporting measured
publish latency percentiles, delivery counts, queue effects, and machine metadata. The
runner derives its authentication and connection capacity from the requested participant
count, so it does not inherit the development authenticator's 64-identity limit. It directly
exercises core routing, not the live QUIC/WebTransport adapters or transport worker.

**Inference plane** (`woven-inference-*`) — an optional, adjacent plane, disabled by
default, adding no dependency to the core or protocol crates beyond twelve additive wire
message kinds. A coordinator (`woven-inference-coordinator`) runs each AI identity as
an ordinary authenticated core connection, holding a bounded per-request provider queue. A
provider-neutral capability/request model and `Provider` trait live in
`woven-inference-core`. A deterministic tool-call gateway
(`woven-inference-tools`) lets model output propose state changes without ever
mutating state directly — the model proposes, the gateway decides. A deterministic scripted
provider (`woven-inference-test-provider`) exercises the full path — an AI
conversation, a read-only tool call, and rejection of a stale state-changing proposal —
with no paid service required.

**Reference clients** — the native Rust library (`woven-client`) used as the
integration-test driver, selecting QUIC or WebTransport automatically from the connection
URL scheme; and the TypeScript WebTransport browser package (`@signalweave/woven-client`) that
mirrors the Rust client API over the WHATWG `WebTransport` transport, with encode/decode
tests and a mock-transport client test. The Rust and TypeScript encoders are proven
wire-compatible in both directions against the same checked-in golden fixtures, closing the
cross-language loop. These two are the focused validation surface for now; additional
client languages (previously codec-only C# and Python bindings) are deferred and will be
expanded one at a time after the two stable clients are hardened. Live transport behavior is
exercised by the Rust client (QUIC + WebTransport) and by the TypeScript client in the bounded
headless-Chromium WebTransport E2E that gates CI and release validation.

See [`docs/adr`](adr) for the architecture decisions behind these choices, and
[`AGENTS.md`](../AGENTS.md) for exact public APIs.

## Out of scope for this repo

Cloud deployment, orchestration, and any hosted console/control-panel UI are deliberately
kept out of this repository so the core stays agnostic and self-hostable on its own.
Woven exposes what an external orchestrator needs — health/readiness endpoints,
`/v1/capabilities`, and bounded, explicit resource configuration — without assuming one
exists. Domain-specific consumer examples (a game namespace, a portfolio site, etc.) are
likewise left to consuming projects: no game rules, physics, or domain logic belong in
`woven-core` or `woven-protocol`.
