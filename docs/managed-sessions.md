# Managed sessions: local integration contract

**Status: managed runtime/admin API, native QUIC plus optional WebTransport WVN1 bridges, and Rust/TypeScript admission clients implemented and locally tested within the limits below.**
`woven_server::{ManagedServerConfig, start_managed, serve_managed}` provides the opt-in
composition described below. Host owns endpoint selection, external server IDs, entitlements,
and secret distribution.
Woven receives only explicit scope, capacity, optional publish ceilings, and credentials over a network API.
No hosted product/tier names or account models enter core.

## Composition and trust boundary

- A separate, opt-in managed composition is available. Native QUIC is mandatory and
  WebTransport is optional. Existing development and static `RemoteServerConfig` behavior remain
  unchanged; managed and static modes are mutually exclusive. Managed mode starts with no
  sessions or client tokens.
- Required environment: `WOVEN_MANAGED_QUIC=1`, `WOVEN_QUIC_BIND`,
  `WOVEN_MANAGEMENT_BIND` (existing read-only loopback HTTP), `WOVEN_ADMIN_BIND` (separate
  loopback listener), `WOVEN_TLS_CERT_FILE`, `WOVEN_TLS_KEY_FILE`, and
  `WOVEN_ADMIN_TOKEN_FILE`. Missing, malformed, mixed-mode, or partial configuration fails before
  *any* listener binds. No fallback to development credentials or certificates.
- Optional managed WebTransport is all-or-nothing:
  `WOVEN_MANAGED_WEBTRANSPORT=1`, `WOVEN_WEBTRANSPORT_BIND`,
  `WOVEN_WEBTRANSPORT_PATH`, and `WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS`. It binds a separate,
  explicit UDP endpoint; managed mode does not apply the development QUIC-plus-one convention.
  The path is an exact absolute path of at most 256 bytes with no query or fragment. The entire
  comma-separated origin setting is at most 2,048 bytes and contains 1–64 exact canonical
  HTTP(S) URL origins. Schemes and hosts must already be lowercase, default ports must be omitted,
  and paths (including a trailing slash), queries, fragments, credentials, whitespace, and
  duplicates are rejected. Browser requests require an exact allowed `Origin`; missing origins
  are rejected.
- The admin listener is loopback-only in this slice. Host on another machine must
  use an operator-configured authenticated encrypted tunnel/private gateway to it;
  direct private-IP HTTP and public admin exposure are not supported. Host supplies
  its management base URL; Woven never discovers a Host endpoint.
- Every admin route, including reads, requires `Authorization: Bearer <admin-token>`.
  The admin credential is independent of client tokens and is never accepted by
  QUIC. Client credentials never authorize management. Reject ambiguous/duplicate
  authorization headers. Authenticate before scope lookup and body processing.
- Preserve the existing read-only loopback router without mutations. Never mount
  `woven_server::admission::routes()` on either composition: its standalone
  controller, caller principal, and ignored server path are not this boundary.
- Load bounded regular secret files with restrictive Unix permissions as in remote
  mode. Validate all inputs and TLS before binding. Never log authorization headers,
  request bodies, token digests, or debug representations containing credentials.

## HTTP contract (v1)

JSON uses camelCase. All `u64` IDs/revisions are canonical nonzero decimal **strings**
(to avoid JavaScript precision loss); `allocatedCCU` is a nonnegative `u32` number.
Reject unknown fields, zero/overflow IDs, oversized bodies, and invalid tokens.

| Method and path | Request | Result |
|---|---|---|
| `GET /v1/node` | Admin auth | `nodeIncarnation`, transport status, limits and supported fixed channel/space definitions |
| `GET /v1/logs?after=0&limit=32` | Admin auth and exact current `Woven-Node-Incarnation` on every read | Bounded volatile node/client log feed; contract below |
| `PUT /v1/namespaces/{namespaceId}/sessions/{sessionId}` | `{ "revision": "1", "allocatedCCU": 1, "clientToken": "<Host-supplied secret>", "tickRateHz": 60 }` (`tickRateHz` optional) | `201` created; identical live retry `200` |
| `GET /v1/namespaces/{namespaceId}/sessions/{sessionId}` | Admin auth | `200` sanitized configuration and admission snapshot; `404` absent |
| `PATCH /v1/namespaces/{namespaceId}/sessions/{sessionId}` | `{ "revision": "2", "allocatedCCU": 2, "tickRateHz": 30 }` (`tickRateHz` optional) | `200` applied configuration/snapshot |
| `PUT /v1/namespaces/{namespaceId}/sessions/{sessionId}/spaces/{spaceId}` | Strict 3D spatial definition below | `201` added; exact same-revision/same-definition retry `200` |
| `DELETE /v1/namespaces/{namespaceId}/sessions/{sessionId}` | `If-Match: "2"` (current revision) | `204` revoked and removed; identical delete retry `204` |

All mutations also require `Woven-Node-Incarnation`, matching the authenticated
`GET /v1/node` response. Its `transports.webTransport` object always includes `enabled`; when
true it also includes `certificateSha256`, the 64-character lowercase SHA-256 digest of the DER
bytes of the actual first certificate installed in the live WebTransport TLS chain. Disabled
responses omit the hash. `/v1/node` never returns transport addresses or endpoint URLs; Host must
combine this identity metadata with separately configured externally reachable endpoints. The
node creates a fresh non-secret random incarnation at startup. A stale incarnation returns `409`;
clients must not automatically replace it and replay a mutation. Host must deliberately reconcile
after restart.

Managed `GET /metrics` on the private, read-only loopback management listener returns
`Woven-Node-Incarnation` as a response header: the existing 48-lowercase-hex identity,
paired with the same run's unchanged Prometheus counter sample. Each new managed node
start changes the incarnation and resets the in-memory counters. Host can deduplicate
samples per configured target and incarnation to retain observed lifetime totals;
Woven does not persist them. Preserve the header through private gateways. This is
HTTP response metadata only, with no WVN1 wire-protocol or binding change.

Provisioning installs one exact `SessionKey`, mandatory admission, and fixed logical
broadcast compatibility/system spaces 1/2, epoch 1. Both spaces expose exactly `channelIds: ["1", "4"]`:
channel 1 is ReliableOrdered/Ephemeral and channel 4 is
UnreliableSequenced/Ephemeral, each with a 64 KiB payload ceiling. Managed mode does not
register or advertise a Stateful channel. No client may override policy. Provisioning
is an atomic worker operation, not a sequence observable between commands. It does
not authenticate/join a connection. The node never implicitly provisions a requested
scope. Static remote, development, and other self-hosted compositions retain their
independent channel definitions and the generic engine retains Stateful support.

Channel 4 carries opaque `EntityState` with a server-assigned owned entity and a
nonzero payload type/coalescing component. Persistence is resolved from the
registered channel through the bounded worker, not selected by clients or inferred
from `EntityState`. Ephemeral updates are not cached or replayed to late joiners.
QUIC and WebTransport accept client datagrams only for
`EntityState/UnreliableSequenced`; control and reliable traffic use the stream.
All outbound paths use datagrams for UnreliableSequenced delivery. The complete encoded
frame must fit the negotiated datagram budget; unsupported, oversized, or failed
sends are dropped without stream fallback. Stale/duplicate unreliable sequences
remain rejected but do not close the connection. Other publish failures retain
their existing rejection behavior.

Host integrations validating the former one-channel contract must update their
exact node schema and returned per-space channel IDs before consuming channel 4.
No change to session PUT/PATCH, capacity, credentials, or admission is required.

### Optional session publish ceiling (`tickRateHz`)

`tickRateHz` is an integer JSON number in **1..120**. Zero, 121, fractions (including `1.0`),
strings, booleans, and explicit `null` are invalid (`400`,
`{"error":{"code":"invalid_request"}}`), without changing capacity, revision, credentials, or
runtime configuration. It is a generic **publish-admission budget per connected session member
per second**, not a physics/simulation tick scheduler. Woven does not generate domain ticks,
interpret payloads, or own product-type policy; consumers and Host retain those responsibilities.

- PUT omission creates a session without an additional session ceiling. The existing core
  per-connection publish limit still applies across all sessions (default 256 publishes/second).
  PUT identity includes the optional rate: an otherwise identical same-revision PUT with a
  different rate, or with the configured rate omitted, returns `409 revision_conflict`.
- PATCH still requires `revision` and `allocatedCCU`. Omitted `tickRateHz` preserves the existing
  value, including unlimited sessions; PATCH cannot clear a configured ceiling. Identical
  same-revision retries return `200` without reapplying configuration. Retry identity includes
  capacity and the requested optional rate, so changing either the rate or its presence at the
  same revision returns `409 revision_conflict`; stale revisions also return `409`.
- PUT/GET/PATCH and spatial-PUT snapshots include top-level `"tickRateHz": 60` only when
  configured. Unlimited snapshots omit the property entirely (never `null`), preserving old
  request/response shapes. Existing IDs/revisions, `nodeIncarnation`, `allocatedCCU`, `spaces`,
  and `admission` fields are unchanged. Spatial adds share the revision and preserve the ceiling.
- All publish channels and spaces in one session consume the **same** member budget, including
  reliable stream events and unreliable state datagrams. Other connections and sessions have
  independent budgets; no per-channel allowance multiplies the ceiling. The core connection-wide
  limiter remains an additional cap. Subscription/control operations do not consume this budget.
- Enforcement reuses the worker-owned fixed-window limiter: a one-second window starts on the
  member's first eligible publish, and the next publish at or after its exact end starts a new
  window. Up to the configured count may be admitted anywhere inside that window; this is not
  pacing or a sliding-window guarantee. Eligible publish attempts reserve budget before later
  channel/payload/sequence/authority validation, without refunds, just as the core limiter does.
  Rate rejects do not mutate sequences, routing positions, cached state, or outbound deliveries.
- Window starts and counts survive PATCH, identical retries, capacity changes, spatial adds,
  rate increases/decreases, and **LeaveSession followed by re-admission/rejoin and resubscription
  on the same connection**. Leaving still releases admission and removes entities, subscriptions,
  sequences and queued messages; it does not replenish the publish budget. Publishes are tracked
  even while unlimited, so enabling a ceiling mid-window cannot replenish the budget. Lowering
  below the consumed count blocks further publishes until the window ends; increasing allows
  only the remaining difference.
- Rate state is connection-owned and bounded by `CoreConfig::max_memberships_per_connection`
  (default **32**). Active memberships and retained detached histories share these slots. Before
  a join, expired detached histories are lazily pruned; empty histories with no publish window
  are immediately eligible for pruning. Active budgets and unexpired detached budgets are never
  evicted to make room. A new session needing another slot fails with the existing
  `CoreError::MembershipLimitReached`, without joining or leaking an admission lease. Rejoining
  a retained session is allowed even when the history cap is full, subject to normal membership,
  authorization and admission rules. Generic multi-session callers may need to wait for an
  inactive window to expire before visiting another session, even with free membership slots;
  managed connections are already scoped to one session and reuse their one history slot.
- If a queued claim obtains a lease but atomic membership binding fails (including the history
  cap), the worker releases that exact lease without a reconnect reservation and invalidates
  both its connection-owned cached ticket and controller ticket. The original binding error is
  returned; subsequent operations on that ticket report `Missing`, not `Admitted`. After a history
  slot expires, request fresh admission (the same idempotency key may be reused). Successful
  admitted requests/claims retain their existing idempotency and do not allocate extra leases.
- A session that has never had a publish ceiling discards its history on leave: unlimited-only
  session churn retains its old behavior. Once a ceiling has been configured, history survives
  leave even through later unlimited periods. Managed history expires at its one-second boundary;
  generic core policies use their configured window. Expiry cleanup cannot shorten an active
  window or discard a clock-regressed history. Transport loss removes the entire connection and
  its histories. Managed DELETE closes all authenticated scope connections, including detached
  ones, reclaiming their histories along with the session.
- Rejection is the existing `CoreError::PublishRateLimited { retry_after }`, mapped over both
  managed transports to WVN1 `ProtocolErrorCode::RateLimited` for the rejected publish kind.
  Existing transport rejection/connection-close behavior is unchanged; no silent retry, delivery,
  or reliable fallback is added. This admin HTTP addition does **not** change WVN1 or require
  regenerated protocol/client bindings.

The trusted Rust API adds `tick_rate_hz: Option<u32>` to both `ManagedRequest::Put` and
`ManagedRequest::Patch`, and `ManagedSnapshot::tick_rate_hz: Option<u32>` (serialized only for
`Some`). Direct core embeddings can call
`WovenCore::set_session_publish_rate_limit(session: SessionKey, policy: Option<PublishRateLimit>)
-> Result<(), CoreError>`; managed HTTP maps Hz to `max_publishes = Hz`, `window = 1s`.
`WorkerHandle::manage(ManagedRequest)` and `WovenCore::manage_at(ManagedRequest, Instant)` retain
their signatures and serialize rate/capacity/revision changes with admission and client commands.
`join_session_at(connection, session, Instant)` and
`join_session_with_admission_at(connection, session, AdmissionLease, Instant)` add deterministic
history-expiry seams; existing join methods remain wall-clock wrappers. Managed admission and
queue claims forward their injected time through the atomic join.
No credentials, scopes, admission leases, or product/domain logic are changed.

### Best-effort log collection

`GET /v1/logs` belongs only to the bearer-authenticated independent admin listener,
not the unauthenticated read-only listener. Every request requires one matching
`Woven-Node-Incarnation` header (`409` for missing/stale/duplicate incarnation).
Both parameters are required: `after` is a canonical decimal `u64`, including `0`;
`limit` is canonical decimal 1–32. Unknown, duplicate, percent-encoded, malformed,
overflow, or noncanonical parameters and nonempty bodies return `400`. Existing
admin concurrency/rate limits, timeout, and `no-store` apply.

The exact response shape agreed for Host collection is:

```json
{
  "nodeIncarnation": "current-incarnation",
  "entries": [{
    "sequence": "1",
    "occurredAtMs": 1800000000000,
    "namespaceId": "1",
    "sessionId": "1",
    "connectionId": "1",
    "source": "client",
    "event": "client.log",
    "level": "info",
    "message": "example client diagnostic"
  }],
  "nextSequence": "1",
  "droppedThrough": "0"
}
```

`source` is `client|node`; `event` is
`client.log|client.connected|client.disconnected`; `level` is `info|warn|error`.
IDs and cursors are decimal strings; `occurredAtMs` is numeric server capture time.
A single-owner worker retains **2,048** entries globally. Sequences are monotonic,
nonzero and never wrap per incarnation. `droppedThrough` is the highest evicted
sequence (`"0"` initially). Return entries strictly after `after` in order;
`nextSequence` is the last entry actually returned, or the supplied `after` when
empty. The node dynamically reduces the page count to keep **all serialized JSON
at or below 48 KiB**, including actual JSON escaping, below the Host 65,536-byte
ceiling. Collection does not consume the ring; continue with `nextSequence` and
use `droppedThrough` to detect missing history. The authenticated administrator
can read all node tenants; clients cannot read or relay this feed.

`ClientLog` uses kind 40 and negotiated capability mask `CAPABILITY_CLIENT_LOG = 2`. It requires an actually authenticated,
joined membership for the asserted session; grants/admission tickets alone are
insufficient. Messages contain 1–1,024 UTF-8 bytes and Info/Warn/Error. Capture is
limited to 10 per connection and 1,024 aggregate per rolling second. Node entries
record only first successful membership (including immediate admission and queue
claim), and membership loss through leave, transport loss, revoke or slow-consumer
cleanup. Failed/queued admission, retries, and publish/update traffic add no node
entries. IDs/scope/time are server-attached; node text is fixed, with no credential,
application payload, or client-provided leave-reason tracing.

No database/cloud/disk work occurs in Woven. This buffer is best-effort pending Host
collection, not durable persistence or a persistence acknowledgement; eviction,
shutdown and restart can lose logs. Reconcile a new incarnation deliberately.
Bridge-level log rejections are nonfatal `ProtocolError`s. At present the transport
adapters close on protocol decode failures, including oversized wire `ClientLog`
messages rejected before the bridge; nonfatal handling of that case remains an
adapter read-loop follow-up.

### Add-only managed spatial subspaces

A live managed session accepts up to **64 additional** spatial spaces beyond system spaces 1/2,
for **66 total managed spaces**. The path `spaceId` is a canonical nonzero decimal string and must
not be 1 or 2. The request is strict JSON (unknown or omitted fields reject):

```json
{
  "revision": "2",
  "metersPerUnit": 1.0,
  "cellSize": 10.0,
  "interestRadius": 15.0,
  "exactDistance": true,
  "bounds": {
    "min": { "x": -1000.0, "y": -1000.0, "z": -1000.0 },
    "max": { "x": 1000.0, "y": 1000.0, "z": 1000.0 }
  }
}
```

`revision` is the next session-wide revision. `metersPerUnit`, `cellSize`, and
`interestRadius` must be finite and greater than zero. Every bound coordinate must be finite and
must satisfy `min.x < max.x`, `min.y < max.y`, and `min.z < max.z`. Runtime position validation is
an **inclusive** AABB: coordinates exactly equal to either min or max are valid. The values above
are examples/reasonable UI defaults, not server-side omission defaults; clients must send every
field explicitly.

The server fixes `CoordinateFrame::Cartesian3D`, `RoutingPolicy::SpatialGrid3D`, no parent,
epoch **1**, and channel IDs **1 and 4**. A caller cannot request 2D, another epoch, nesting, or
channel policy. The add and all grant updates happen in one owning-worker turn. Already
authenticated connections for the session gain exact read/write space and channel grants, and
future connections receive the same exact grants. There is no wildcard space grant: subscriptions
and publishes to unknown/ad-hoc space IDs remain unauthorized.

Spaces are add-only for the node incarnation: there is no space PATCH or DELETE route, and an
existing `spaceId` cannot be redefined. An exact same-revision/same-definition retry returns `200`;
the same revision with a different definition, a lower revision, or any attempted mutation returns
`409 revision_conflict`. Adding the 65th spatial space returns
`409 space_capacity_exhausted`. Session PATCH, space PUT, and session DELETE all share one revision
sequence; callers must serialize changes and reconcile from the sanitized session response.

Successful session GET/PUT/PATCH and space PUT responses expose `spaces`. Every definition includes
`spaceId`, fixed `epoch`, and fixed `channelIds`; spatial definitions additionally include
`metersPerUnit`, `bounds`, `cellSize`, `interestRadius`, and `exactDistance` as flat fields.
`GET /v1/node` advertises `capabilities.positionedEntityState`,
`capabilities.managedSpatialSubspaces`, `limits.maxAdditionalManagedSpatialSpaces = 64`, and
`limits.maxManagedSpaces = 66`; its top-level spaces remain the fixed system definitions 1/2.

Successful GET/PUT/PATCH responses contain `nodeIncarnation`, `namespaceId`,
`sessionId`, `revision`, requested `allocatedCCU`, and `admission` with
`effectiveAllocatedCCU`, `pendingTarget`, `activeCCU`, `offeredSlots`,
`queueDepth`, and `availableSlots`. A decrease drains naturally using the existing
pending-target semantics. Zero pauses new admissions. Existing leases are not
kicked by capacity reduction; DELETE is the explicit teardown operation.

Host supplies exactly one random server-specific token: 32 random bytes encoded as
64 lowercase hexadecimal characters. This document does not create any secret.
The node retains only a SHA-256 verifier, rejects reuse across scopes and reuse of
the admin credential, and never returns/echoes the client token. Host already owns
it, so lost responses do not require storing plaintext or issuing another token.
Token validation proves possession, not an individual user's identity.

Session PUT is create-only: the same live revision, capacity, and token verifier is an
idempotent retry; any difference returns `409`. Space PUT follows the add-only idempotency rules
above and advances the same session revision. PATCH requires a higher revision;
an identical current-revision retry succeeds, conflicting/stale revisions return
`409`. PATCH cannot rotate credentials or change scope. DELETE requires the current
revision and records a tombstone with the deleted verifier and revision. That scope
and token cannot be reused during the node incarnation. Repeated matching DELETE
succeeds; stale updates/PUT cannot resurrect it. Unknown DELETE returns `404`.
Bound tombstones and reject new provisioning when history is full rather than
silently forgetting revocation. Host must use fresh tokens on reconciliation after
restart; this in-memory slice does not promise durable token-reuse prevention.

Errors use `{ "error": { "code": "..." } }`, with no secrets or caller input:
`400 invalid_request`, `401 unauthorized`, `404 scope_not_found`,
`409 revision_conflict|incarnation_conflict|scope_retired|token_conflict|space_capacity_exhausted`,
`413 request_too_large`, `429 rate_limited`, `503 capacity_exhausted|worker_unavailable`.
Responses use `Cache-Control: no-store`. No HTTP admission routes are exposed.

## Managed WVN1 admission contract

Use `Client::connect_with_tls_and_auth(config, tls, AuthenticationScheme::Bearer)` for native
QUIC, or configure the TypeScript `WovenClient` with `authenticationScheme:
AuthenticationScheme.Bearer` for WebTransport. Managed QUIC and WebTransport both enforce that
scheme before passing credentials to the worker: a valid managed token labeled `Development` is
rejected as unauthorized. Existing development and static remote compositions retain their
Development-compatible defaults. Rust certificate-chain, validity, and URL hostname/IP SAN
verification remain enabled, including on loopback; there is no insecure remote fallback. This
is opaque shared credential authentication, not JWT validation or per-user identity.

A verified managed token grants only its exact namespace/session and configured
spaces/channels. Each authenticated connection receives a distinct server-assigned
principal, even when connections use the same token. No client principal field is
accepted. Each connection is scoped to one managed session, including before join.

Implemented additive WVN1 session-scoped ReliableOrdered controls use message kinds
33–39 (control-union tags 30–36), respectively, in the following order. All require
nonzero namespace/session and correlation IDs, with no space/channel/entity/epoch
scope or domain payload; replies echo scope and correlation:

- `RequestAdmission { idempotencyKey }` -> `AdmissionResult` with admitted, queued,
  paused, or a structured rejection.
- `QueueStatusRequest { ticketId }`, `QueueHeartbeat { ticketId }`,
  `QueueClaim { ticketId }`, `QueueCancel { ticketId }` -> a correlated `QueueUpdate`
  with waiting/position, offered, admitted, cancelled, expired, or missing.
- Replies include bounded `pollAfterMs` (currently 1,000 ms for queued/paused or
  waiting/offered states). Remaining ticket/offer lifetime fields are currently zero:
  **unavailable**, not a fresh TTL, because the worker returns state only.
  Polling/heartbeats observe offers; there is no separate offer push task.

Authentication, scope, and rate failures produce a correlated `ProtocolError` and
close the connection rather than fabricating admission outcomes. Client-sent result
controls are rejected. No lease, principal, or resume token is exposed by these controls.

Direct admission and successful claim atomically bind the lease and join the
session in the worker. A client never supplies an admission lease. Subscription,
entity creation, and publishing remain separate operations. Ordinary `JoinSession`
continues to reject managed sessions with admission-required; no compatibility
path bypasses admission. Legacy sessions without admission retain existing joins.

The worker verifies scope and `(connection, session, ticket)` ownership on *every*
queue operation, including status/heartbeat/cancel. Guessing a ticket number grants
nothing. Foreign and nonexistent tickets both return missing. Idempotency is scoped
to the verified connection and session, not globally keyed by client strings.
Repeating an admitted request does not allocate another lease; repeated claims do
not increment CCU. One connection has at most one live ticket/lease for its scope.

The managed slice uses zero reconnect grace, deliberately overriding the controller's
20-second default. Disconnect/leave releases its lease exactly once and immediately
makes a slot offerable to the next waiter. Disconnected waiters are cancelled.
No resume token is exposed or accepted in managed mode: connection-specific identity
cannot securely resume through the existing counter-valued controller resume token.

DELETE first revokes verification and invalidates all scope operations in one worker
turn, removes all tickets/leases/entities/state/queued messages, and closes *all*
authenticated scope connections, including those not joined or waiting. Lifecycle
fan-out must terminate native QUIC, not merely detach core membership. Success means
revocation and local close initiation, not proof the peer has received a close packet.
Commands already in flight are serialized before or after deletion, never against
partially removed state. Old authenticated connections cannot survive reprovisioning.

## Implemented bounds and client constraints

Fixed defaults for this local slice, validated against node hard limits:

- At most 64 add-only managed 3D spatial subspaces per scope, in addition to fixed logical spaces
  1/2 (66 total). Definitions and exact grants are bounded and in memory for the node incarnation.
- At most 1,024 live scopes and 4,096 total live/retired scope verifiers per incarnation;
  creation reserves a history slot so DELETE cannot fail for lack of tombstone space;
  per-scope allocation at most the node's 4,096 connection ceiling. Admission
  allocations do not guarantee the node can serve their aggregate concurrently.
- At most 1,024 waiting/offered tickets per scope, with additional node-wide live
  ticket/connection limits. At most 1,024 terminal tickets per scope retained for
  60 seconds; evict oldest terminal entries, never live permits. Expired/evicted
  retries return missing and do not recreate a ticket automatically.
- Ticket lifetime 15 minutes, heartbeat timeout 30 seconds, offer TTL 30 seconds;
  poll advice 1 second, at most 4 admission operations/second/connection. A bounded
  worker maintenance tick (100 ms) advances expiry/promotion without inbound traffic.
- Admin body 8 KiB, 32 concurrent requests, 32 requests/second, 5-second deadline;
  worker submission uses a bounded mailbox. Timeout can mean applied-with-response-
  lost: retry identical mutations, never synthesize a new revision on timeout.
- Rust clients expose `request_admission`, `queue_status`, `queue_heartbeat`, `queue_claim`,
  and `queue_cancel`; the TypeScript client exposes the corresponding camelCase methods. Each
  operation has a ten-second exchange timeout. These are exclusive pre-subscription exchanges,
  not a multiplexed application inbox. Unexpected traffic, transport errors, and timeouts fail
  closed; after externally cancelling a borrowed exchange, close/drop the client rather than
  reusing it.
- Rust `admit_with_cancellation` and TypeScript `admitWithCancellation` consume a fresh
  authenticated client, with a caller-set positive deadline of at most 15 minutes and
  heartbeat/claim polling clamped to 1–5 seconds. They perform **zero transport retries** because
  partial stream I/O cannot safely be replayed. Semantic outcomes are returned without retrying.
  Cancellation, deadline, or I/O error closes/drops the connection, including races with an
  admitted claim. `queue_cancel`/`queueCancel` does not undo an already admitted session:
  leave/disconnect to release the lease.

Runtime integration uses the authoritative core/worker, not an HTTP-side controller.
`WorkerHandle::manage(ManagedRequest)` serializes PUT/GET/PATCH/DELETE with client
commands. `WovenCore::enable_managed` disables fallback authentication; the worker
owns SHA-256 verifiers, revision history and authenticated-scope connection tracking.
`Command::RequestSessionAdmission` now calls `admit_and_join_session_at`: admitted
results already have membership bound atomically. A follow-up
`JoinSessionWithAdmission` remains idempotent but is unnecessary. `SessionQueue`
claims are also atomic. Managed admission operations are limited to four/second per
connection (`CoreError::AdmissionRateLimited { retry_after }`); ordinary static/dev
sessions do not acquire this new limit.

The runtime tests in `woven-core/tests/managed.rs` and `woven-server/tests/managed.rs` cover HTTP
response fields, credential isolation, worker queue/claim, verified QUIC teardown,
revision/history exhaustion, configuration, request deadlines and bounds, add-only spatial
capacity/idempotency, live/future exact grants, positioned datagram traffic, inclusive bounds, and
the cross-cell-boundary routing regression. `woven-core/tests/session_publish_rate.rs` adds
injected-time boundary, cross-channel/space aggregation, connection/session isolation, preserved
windows, global-limit, and membership cleanup checks. Managed runtime/HTTP tests cover optional
rate snapshots, malformed Hz, atomic validation, retries/stale revisions and legacy omission;
real managed QUIC and WebTransport tests verify a reliable event consumes the same budget as a
subsequent channel-4 datagram, which receives the existing rate error.
`woven-server/tests/managed_quic.rs` additionally exercises the real TLS-verified WVN1
bridge/native client: Bearer enforcement and Development rejection, the Ephemeral-only channel
policy, distinct principals, correlated rate errors, CCU-one queue/heartbeat/claim, duplicate
operations, ticket ownership/cancellation, scope isolation, ordinary-join rejection, DELETE
teardown, and bounded helper cancellation. `woven-server/tests/managed_webtransport.rs` uses a
real TLS-verified WebTransport socket and explicit `Origin` to cover Bearer enforcement and
Development rejection, exact-origin admission, direct/queued admission and claim, atomic
admission/join then subscription, wrong token/scope, DELETE and server-drop teardown, capability
metadata, and the leaf fingerprint returned by `/v1/node`. Protocol tests cover semantic
validation and the additive `queue_update_v1` golden.

The cross-repository `woven-server/tests/host_managed_local.rs` E2E is implemented
and has passed via `npm run test:local` from `../woven-host` (relative to
Woven's root). It runs the real Host HTTP API with isolated Firebase Auth/Firestore
emulators and the real managed node on loopback. Host provisions through authenticated
admin HTTP and returns descriptors used by the public TLS-verified native QUIC client.
Coverage includes owner/scope isolation, ten admitted clients and an eleventh queued,
Host capacity/monitoring, disconnect/offer/claim, TLS trust/name rejection, server
deletion and account teardown with socket closure and token revocation. This test is
ignored by ordinary Cargo runs; use the Host launcher, not the helper directly.
It does not enable managed WebTransport and does not validate browser UI, TypeScript network
traffic, Host-provided WebTransport descriptors, production Firebase/App Check, cloud deployment,
Weaver integration, persistence/restarts, or multi-node behavior.

Rust bindings are generated at build time; checked-in TypeScript bindings and codec support
include all seven controls plus additive positioned-state metadata. The TypeScript WebTransport
client implements `requestAdmission`, all four queue operations, bounded `admitWithCancellation`,
and reliable/unreliable positioned-state APIs with fail-closed capability checks. Tests include an
in-memory WHATWG WebTransport mock, Rust/TypeScript wire-compatibility fixtures, and a bounded
headless-Chromium connection to a disposable managed server. Browser WebTransport/TypeScript is
therefore **Online** for this bounded protocol surface. Managed WebTransport is a separate
explicitly configured endpoint; do not infer it from a managed native QUIC URL. Regenerate
bindings and golden fixtures after schema changes. The local Host E2E above remains native QUIC
only; Host-provided browser descriptors, Weaver integration, and external remote deployment remain
untested.

Local validation coverage spans the following boundaries (core/worker tests for
expiry and bounds, real loopback QUIC tests for the network path):

1. Missing/wrong client and admin tokens, swapped credentials, cross-scope access,
   unknown scope and deleted credentials all fail without creating state.
2. CCU 1: two real TLS-verified QUIC clients with the same token have distinct principals;
   second waits, first disconnects, second observes offer and claims. Real WebTransport coverage
   separately exercises the direct-admission and queue/offer/claim paths.
3. Forged/cross-connection tickets cannot inspect, heartbeat, cancel, or claim;
   duplicate requests/claims do not leak permits. Ordinary join cannot bypass CCU.
4. Expiry, churn, retry, and history-exhaustion tests prove all collections bounded.
5. Partial/invalid environment and invalid TLS/secrets bind no listeners; read-only management
   never exposes admin routes, including during failed startup. Configuration tests bound the
   total origin setting and collected entry count, and reject noncanonical scheme/host case,
   explicit default ports, paths/trailing slashes, queries, fragments, credentials, duplicates,
   missing values, and invalid request paths.
6. Capacity updates/replays/decreases and provisioning rollback preserve atomicity.
7. DELETE closes admitted, waiting, and authenticated-not-joined sockets, releases
   all resources, revokes old tokens, and defeats stale create/update/delete replays.
8. Wrong CA/hostname fails native TLS; legacy development/static remote tests pass. Managed QUIC
   and WebTransport reject the Development scheme even when the token is otherwise valid.
9. Managed WebTransport rejects missing/wrong origins, reports only local capability metadata on
   the read-only listener, and reports enabled state plus the live leaf SHA-256 fingerprint—but no
   endpoint URL—through authenticated `/v1/node`.

No cloud actions, production secrets, deployments, durable managed persistence, failover,
external managed WebTransport deployment, Host-provided browser descriptor E2E, or production
per-user identity are part of this Woven slice. Host implementation remains in the sibling repository; Weaver
integration remains separate.
