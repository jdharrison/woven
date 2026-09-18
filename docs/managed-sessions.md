# Managed sessions: local integration contract

**Status: managed runtime/admin API, WVN1 bridge, and native QUIC admission client implemented and locally tested.**
`woven_server::{ManagedServerConfig, start_managed, serve_managed}` provides the
opt-in composition described below. Host owns endpoint selection, external server
IDs, entitlements, and secret distribution.
Woven receives only explicit scope, capacity, and credentials over a network API.
No hosted product/tier names or account models enter core.

## Composition and trust boundary

- A separate, opt-in managed native QUIC composition is available. Existing development and
  static `RemoteServerConfig` behavior remain unchanged; managed and static modes
  are mutually exclusive. Managed mode starts with no sessions or client tokens.
- Required environment: `WOVEN_MANAGED_QUIC=1`, `WOVEN_QUIC_BIND`,
  `WOVEN_MANAGEMENT_BIND` (existing read-only loopback HTTP),
  `WOVEN_ADMIN_BIND` (separate loopback listener), `WOVEN_TLS_CERT_FILE`,
  `WOVEN_TLS_KEY_FILE`, and `WOVEN_ADMIN_TOKEN_FILE`.
  Missing, malformed, mixed-mode, or partial configuration fails before *any*
  listener binds. No fallback to development credentials or certificates.
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
| `GET /v1/node` | Admin auth | `nodeIncarnation`, limits and supported fixed channel/space definitions |
| `PUT /v1/namespaces/{namespaceId}/sessions/{sessionId}` | `{ "revision": "1", "allocatedCCU": 1, "clientToken": "<Host-supplied secret>" }` | `201` created; identical live retry `200` |
| `GET /v1/namespaces/{namespaceId}/sessions/{sessionId}` | Admin auth | `200` sanitized configuration and admission snapshot; `404` absent |
| `PATCH /v1/namespaces/{namespaceId}/sessions/{sessionId}` | `{ "revision": "2", "allocatedCCU": 2 }` | `200` applied configuration/snapshot |
| `DELETE /v1/namespaces/{namespaceId}/sessions/{sessionId}` | `If-Match: "2"` (current revision) | `204` revoked and removed; identical delete retry `204` |

All mutations also require `Woven-Node-Incarnation`, matching the authenticated
`GET /v1/node` response. The node creates a fresh non-secret random incarnation at
startup. A stale incarnation returns `409`; clients must not automatically replace
it and replay a mutation. Host must deliberately reconcile after restart.

Provisioning installs one exact `SessionKey`, mandatory admission, and fixed logical
broadcast spaces 1/2, epoch 1. Both spaces expose only channel 1,
ReliableOrdered/Ephemeral with a 64 KiB payload ceiling. Managed mode does not
register or advertise a Stateful channel. No client may override policy. Provisioning
is an atomic worker operation, not a sequence observable between commands. It does
not authenticate/join a connection. The node never implicitly provisions a requested
scope. Static remote, development, and other self-hosted compositions retain their
independent channel definitions and the generic engine retains Stateful support.

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

PUT is create-only: the same live revision, capacity, and token verifier is an
idempotent retry; any difference returns `409`. PATCH requires a higher revision;
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
`409 revision_conflict|incarnation_conflict|scope_retired|token_conflict`,
`413 request_too_large`, `429 rate_limited`, `503 capacity_exhausted|worker_unavailable`.
Responses use `Cache-Control: no-store`. No HTTP admission routes are exposed.

## Native QUIC admission contract

Use `Client::connect_with_tls_and_auth(config, tls, AuthenticationScheme::Bearer)`
with the existing WVN1 Bearer enum and operator-supplied CA roots. Certificate-chain,
validity, and URL hostname/IP SAN verification remain enabled, including on loopback;
there is no insecure remote fallback. Existing `connect`/`connect_with_tls` retain
Development compatibility. The current QUIC adapter passes credentials to the worker
without enforcing Bearer-only scheme selection: managed isolation is enforced by the
scoped token verifier, not by the authentication-scheme label. This is opaque shared
credential authentication, not JWT validation or per-user identity.

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

## Implemented bounds and native client constraints

Fixed defaults for this local slice, validated against node hard limits:

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
- Native clients expose `request_admission`, `queue_status`, `queue_heartbeat`,
  `queue_claim`, and `queue_cancel`, each with a ten-second exchange timeout. These
  are exclusive pre-subscription exchanges, not a multiplexed application inbox.
  Unexpected traffic, transport errors, and timeouts fail closed; after externally
  cancelling a borrowed exchange, close/drop the client rather than reusing it.
- `admit_with_cancellation` consumes a fresh authenticated client, with a caller-set
  positive deadline of at most 15 minutes and heartbeat/claim polling clamped to
  1–5 seconds. It performs **zero transport retries** because partial stream I/O
  cannot safely be replayed. Semantic outcomes are returned without retrying.
  Cancellation, deadline, or I/O error closes/drops the connection, including races
  with an admitted claim; dropping the helper future drops its owned client.
  `queue_cancel` does not undo an already admitted session: leave/disconnect to
  release the lease.

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

The runtime tests in `woven-core/tests/managed.rs` and `woven-server/tests/managed.rs`
cover HTTP response fields, credential isolation, worker queue/claim, verified QUIC
teardown, revision/history exhaustion, configuration, request deadlines and bounds.
`woven-server/tests/managed_quic.rs` additionally exercises the real TLS-verified
WVN1 bridge/native client: the Ephemeral-only channel policy, distinct principals,
correlated rate errors, CCU-one queue/heartbeat/claim, duplicate operations, ticket
ownership/cancellation, scope isolation, ordinary-join rejection, DELETE teardown,
and bounded helper cancellation.
Protocol tests cover semantic validation and the additive `queue_update_v1` golden.

The cross-repository `woven-server/tests/host_managed_local.rs` E2E is implemented
and has passed via `npm run test:local` from `../woven-host` (relative to
Woven's root). It runs the real Host HTTP API with isolated Firebase Auth/Firestore
emulators and the real managed node on loopback. Host provisions through authenticated
admin HTTP and returns descriptors used by the public TLS-verified native QUIC client.
Coverage includes owner/scope isolation, ten admitted clients and an eleventh queued,
Host capacity/monitoring, disconnect/offer/claim, TLS trust/name rejection, server
deletion and account teardown with socket closure and token revocation. This test is
ignored by ordinary Cargo runs; use the Host launcher, not the helper directly.
It does not validate browser UI, production Firebase/App Check, cloud deployment,
Weaver integration, persistence/restarts, or multi-node behavior.

Rust bindings are generated at build time; checked-in TypeScript bindings and codec
support include all seven controls. TypeScript support is **wire codec compatibility
only**, not a managed browser queue client or WebTransport composition. Do not infer
a browser endpoint from a managed native QUIC URL. Regenerate bindings and golden
fixtures after schema changes. The local Host E2E above does not add managed
WebTransport support; Weaver integration and external remote deployment remain untested.

Local validation coverage spans the following boundaries (core/worker tests for
expiry and bounds, real loopback QUIC tests for the network path):

1. Missing/wrong client and admin tokens, swapped credentials, cross-scope access,
   unknown scope and deleted credentials all fail without creating state.
2. CCU 1: two real TLS-verified QUIC clients with the same token have distinct
   principals; second waits, first disconnects, second observes offer and claims.
3. Forged/cross-connection tickets cannot inspect, heartbeat, cancel, or claim;
   duplicate requests/claims do not leak permits. Ordinary join cannot bypass CCU.
4. Expiry, churn, retry, and history-exhaustion tests prove all collections bounded.
5. Partial/invalid environment and invalid TLS/secrets bind no listeners; read-only
   management never exposes admin routes, including during failed startup.
6. Capacity updates/replays/decreases and provisioning rollback preserve atomicity.
7. DELETE closes admitted, waiting, and authenticated-not-joined sockets, releases
   all resources, revokes old tokens, and defeats stale create/update/delete replays.
8. Wrong CA/hostname fails native TLS; legacy development/static remote tests pass.

No cloud actions, production secrets, deployments, persistence, failover, remote
WebTransport, or production per-user identity are part of this Woven slice. Host
implementation remains in the sibling repository; Weaver integration remains separate.
