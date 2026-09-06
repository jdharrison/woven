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
messages, including inference/tool-call lifecycle messages, a pinned vendored FlatBuffers
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
exercised by the Rust client (QUIC + WebTransport) and the TypeScript client
(WebTransport).

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
