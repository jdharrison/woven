# Admission, capacity allocation, and usage windows

This document describes the local foundation for capacity allocation, admission queues,
reconnect grace, and usage tracking. It covers the domain semantics, the queue state
machine, and how a Weaver client is expected to interact with these features.

## Scope

For the proposed authenticated management and native QUIC integration boundary, see
[Managed sessions](managed-sessions.md). That contract is design-only; the HTTP routes
below remain an unmounted test adapter, not a supported client admission API.

Everything in this document is implemented in `woven-core` and exposed through a thin
development HTTP adapter in `woven-server`. The following are intentionally **not**
implemented in this milestone:

- Firebase/Firestore integration
- Stripe billing
- Google Cloud provisioning
- Distributed/replicated queue failover
- A production HTTP control-plane sink
- FlatBuffers wire messages for join/queue/offer (the next protocol adapter boundary)

## Domain model

A **virtual server** is a Woven.host control-plane record that maps to one provisioned
Woven `SessionKey`. The host control plane owns account entitlements, capacity pools, billing,
and the mapping from a customer-visible virtual-server ID to that `SessionKey`. It prevents
allocations across an account from exceeding its purchased CCU.

Core receives only an authenticated, monotonic per-session update:

```rust
CapacityUpdate {
    allocated_ccu: u32,
    revision: u64,
}
```

A tenant normally uses one `NamespaceId` and one `SessionId` per virtual server. For example,
an account with 10 CCU can allocate 5 CCU to `tenant-a / production`, 2 CCU to
`tenant-a / staging`, and retain 3 CCU for a later allocation. The global Woven fabric hosts
both sessions; it does not need account or billing identity.

`allocated_ccu == 0` means the server is **paused**. A paused server rejects new join
requests with `JoinDecision::Paused`; it does not create an endless queue.

## Admission controller

`AdmissionController` is a single-owner, transport-neutral, in-memory object keyed by one
`SessionKey`. Callers serialize access through the core worker or an async mutex. All deadline
arithmetic uses saturating/checked operations so a regressed or mocked clock cannot panic the
realtime loop.
It is responsible for:

- Allocated capacity and pending capacity targets
- Active admission leases
- Reconnect reservations
- FIFO waiting queue
- Ticket expiration and cancellation
- Admission offers
- Usage instrumentation

It is **not** responsible for billing, authentication, transport sockets, or Firestore.

### Join decision

```rust
enum JoinDecision {
    Admitted(AdmissionLease),
    Queued(QueueTicket),
    Paused,
    Rejected(RejectionReason),
}
```

A lease carries a `resume_token` that can reclaim its slot during reconnect grace.

## Queue state machine

```text
waiting → offered → admitted
   │         │
   ├─────────┴→ expired
   └──────────→ cancelled
```

- **Waiting**: the client is in line. Heartbeats extend participation up to the configured
  maximum ticket lifetime.
- **Offered**: a slot is reserved for a short `offer_ttl`. The client must claim it.
- **Admitted**: the client has claimed the offer and holds an `AdmissionLease`.
- **Expired**: the ticket TTL, heartbeat timeout, or offer TTL elapsed.
- **Cancelled**: the client explicitly cancelled.

Queue positions are advisory and are recalculated after expiration or cancellation. The
server never trusts a client-provided position.

### Idempotency and duplicate principals

A join request carries an opaque `IdempotencyKey`. Repeating a request with the same key
returns the existing queue state for that ticket. One principal cannot hold two waiting or
offered queue positions for the same server; the second request is rejected with
`AlreadyQueued`.

## Reconnect grace

When an admitted client disconnects unexpectedly, the controller reserves its slot for
`reconnect_grace`. A valid `ResumeToken` can reclaim the lease. The slot is not offered to
queued clients until the grace period expires or the session closes intentionally. Leases
are released exactly once; successful and expired reservations are recorded in usage
windows.

## Capacity allocation changes

- **Increase**: takes effect immediately and promotes the oldest valid queued clients.
- **Decrease below occupied capacity**: stores a pending target. Active clients are never
  disconnected, and reconnect reservations and outstanding offers continue to hold their
  permits. Once enough permits drain naturally, the allocation drops to the pending target.
  No new client is admitted above the pending target while it drains.
- **Stale revisions**: updates with `revision <= current_revision` are ignored.

### Example

```text
allocated=5, active=5, queued=7
allocation raised to 10
five queued clients promoted
active=10, queued=2
```

## Usage tracking

`UsageCounters` provides low-overhead atomic counters per virtual server. It tracks:

- Join attempts, admissions, queued joins, rejections
- Active CCU and peak CCU gauges
- Queue depth and peak queue depth gauges
- Connection-seconds (including sessions spanning window boundaries)
- Reconnect reservations, successful resumes, promoted players
- Expired, cancelled, and abandoned tickets
- Events and bytes (received/delivered/dropped)
- Persistence reads/writes and inference requests (when hooks exist)
- Capacity allocation events and revisions

No bearer tokens, IP addresses, message bodies, player PII, or secrets are recorded.

## Windowed aggregation

`UsageAggregator` collects per-server counters into `UsageWindow` records on a configurable
cadence (default 60 seconds). Each window carries:

```rust
UsageWindow {
    schema_version,
    node_id,
    session: SessionKey,
    window_start,
    window_end,
    sequence,
    capacity_revision,
    allocated_ccu,
    metrics,
}
```

Finalization is synchronous and cheap. Counters roll into the next window without loss at
the boundary. Each window has a stable idempotency identity (`node_id + server_id + start +
sequence`) so sink retries do not duplicate logical usage.

## Usage sink boundary

The `UsageSink` trait is async:

```rust
trait UsageSink {
    async fn append(&self, window: UsageWindow) -> Result<(), UsageSinkError>;
}
```

Provided implementations:

- `MemoryUsageSink`: bounded in-memory ring buffer for deterministic tests.
- `JsonlFileSink`: development append-only file sink. The serialized form never contains
  tokens or PII by construction.
- `NoopUsageSink`: only when explicitly configured for tests.
- `SpoolingUsageSink`: wraps an inner sink with a bounded in-memory spool and retry logic.
  Sustained failure marks sink health as degraded and increments `dropped_windows` when the
  spool overflows; windows are never silently discarded while spool capacity remains.

A future production control-plane sink should acknowledge the window's `idempotency_id()`
to deduplicate retries.

## Operational snapshots

`AdmissionSnapshot` exposes:

- allocated CCU, active CCU, available slots
- reconnect reservations and offered slots
- queue depth, pending capacity target
- current capacity revision

A standalone development HTTP adapter can expose this at
`GET /v1/virtual-servers/{server_id}/snapshot`. It is intentionally not mounted by the normal
server composition because it has no production control-plane authenticator. A production adapter
must resolve the host virtual-server ID to a provisioned `SessionKey` and authorize the
authenticated principal before it delegates to core.

## Queue failure behavior

Queue state is held in memory for this milestone. When a node dies:

- Active connections are already lost.
- Queue tickets become invalid after restart.
- Weaver must request a new join or queue ticket.

Durable or replicated queues are deferred until multi-node failover requires them.

## Weaver integration expectations

Weaver clients should:

1. Call `POST /v1/virtual-servers/{server_id}/join` with an idempotency key.
2. On `Admitted`, connect to the realtime transport.
3. On `Queued`, poll `GET /v1/queues/{ticket}` or heartbeat with
   `POST /v1/queues/{ticket}/heartbeat` using the server-advised `poll_after_ms`.
4. On `Offered`, claim with `POST /v1/queues/{ticket}/claim` before `offer_ttl` expires.
5. Preserve `resume_token` to reclaim a slot after an unexpected disconnect.
6. Cancel with `DELETE /v1/queues/{ticket}` when abandoning the queue.

The next protocol adapter to implement is the FlatBuffers wire encoding for join, queue
status, heartbeat, offer, claim, and structured rejection messages so that Weaver can use
the QUIC/WebTransport transports end-to-end.

## HTTP routes (development)

```text
POST   /v1/virtual-servers/{server_id}/join
GET    /v1/queues/{ticket}
POST   /v1/queues/{ticket}/heartbeat
POST   /v1/queues/{ticket}/claim
DELETE /v1/queues/{ticket}
GET    /v1/virtual-servers/{server_id}/snapshot
```

These routes are an unmounted local test adapter with one controller. A production adapter must
authenticate the caller, resolve the host virtual server to a `SessionKey`, authorize it for that
session, and route ticket operations to that same session controller.
