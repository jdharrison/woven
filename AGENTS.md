# WOVEN — Agent Reference

Read this file first and stop. Do not re-read `docs/bootstrap.md`, ADRs, or source files
unless you need to extend a specific area. Everything an agent needs to orient and act is here.
Source sections below provide exact public APIs so you can write code against them without reads.

---

## Identity and vocabulary

| Term | Meaning |
|---|---|
| WOVEN | Umbrella project |
| Woven Node | Runtime process |
| Woven Protocol | Wire protocol (file identifier `WVN1`) |
| Woven Intelligence | Inference subsystem; optional, adjacent, disabled by default |
| `Namespace` | Project/tenant isolation (e.g. `dark-forest`, `portfolio`) |
| `Session` | Shared realm inside a namespace |
| `Space` | Spatial or logical scope within a session, owns a coordinate frame and routing policy |
| `Entity` | Addressable participant; always server-assigned, always owned by one connection |
| `Channel` | Typed event/state family; delivery class and persistence class are server-controlled |
| `Subscription` | A connection's authorized view into a space |
| `Envelope` | Versioned routing metadata wrapping a typed payload |

IDs: all are `u64` newtypes. **0 is reserved for absent/unassigned on the wire and rejected at every core API boundary.** Assigned IDs start at 1.

---

## Repository layout

```
woven/
├── AGENTS.md                        ← you are here
├── Cargo.toml                       ← workspace root, shared deps/lints
├── Cargo.lock                       ← committed, use --locked in CI
├── rust-toolchain.toml              ← pinned to 1.98.0 stable
├── rustfmt.toml                     ← edition 2024, max_width 100
├── .cargo/config.toml               ← aliases: check-all lint test-all
├── .env.example                     ← WOVEN_* env vars, no secrets
├── .github/workflows/ci.yml         ← format, lint, test, doc, audit
├── docs/
│   ├── bootstrap.md                 ← original project prompt (historical reference)
│   ├── status.md                    ← what's implemented; update when that changes
│   └── adr/0001–0013-*.md          ← architecture decisions (read only when topic-relevant)
└── crates/
    ├── woven-core                    ← transport-neutral sessions, spaces, ownership, queues
    ├── woven-protocol                ← FlatBuffers schema, codec, semantic validation, fixtures
    ├── woven-transport               ← shared worker handle, lifecycle fan-out, protocol bridge
    ├── woven-transport-quic          ← QUIC (Quinn) native + WebTransport browser adapter
    ├── woven-server                  ← Axum control plane, development server composition
    ├── woven-inference-core          ← capability/request/provider data model, Provider trait
    ├── woven-inference-tools         ← bounded tool registry, deterministic tool-call gateway
    ├── woven-inference-test-provider ← deterministic scripted provider for tests/dev
    ├── woven-inference-coordinator   ← runs an AI identity as an ordinary core connection
    ├── woven-client-rust             ← `woven-client` native library, integration-test driver
    ├── woven-client-ts               ← generated TS bindings + real WebTransport browser client (codec, encode, mock-tested)
    └── woven-loadtest                ← bounded local routing scenarios and measurement
```

Do not create empty placeholder crates or modules — every crate above contains working behavior.

---

## Status

Feature-complete for its own scope: transport-neutral core, wire protocol, QUIC and
WebTransport realtime transports, spatial interest routing with a load runner, and an
optional adjacent inference plane. See [`docs/status.md`](docs/status.md) for what's
implemented in each area and [`docs/adr`](docs/adr) for why.

Cloud deployment, orchestration, and any hosted console/control-panel UI are out of scope
for this repository by design — Woven stays agnostic and self-hostable on its own,
the way Redis or Postgres are. Domain-specific consumer examples (games, sites, etc.) are
likewise left to consuming projects.

---

## Validation commands (run before every commit)

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
RUSTDOCFLAGS=-Dwarnings cargo doc --locked --workspace --no-deps
cargo audit
```

Aliases defined in `.cargo/config.toml`: `cargo check-all`, `cargo lint`, `cargo test-all`.

Regenerate the protocol golden fixtures after any schema change:
```sh
cargo run -p woven-protocol --example write_golden
cargo run -p woven-protocol --example write_tool_call_completed_fixture
```

---

## Hard rules (never violate)

- No handwritten `unsafe` code. `woven-core` has `#![forbid(unsafe_code)]`. The protocol crate uses `#![deny(unsafe_code)]` with a narrowly scoped `#[allow]` only inside the private `generated` module (FlatBuffers runtime).
- No unbounded channels, collections, or queues anywhere.
- No global locks. Hot state has a single owner.
- No game rules, physics, or domain logic in `woven-core` or `woven-protocol`.
- No cloud mutations, IAM changes, secret operations, DNS changes, or production deployments without explicit user approval. Local code, tests, containers, infra plans, and read-only inspection are always safe.
- No empty placeholder crates or modules. Add a crate only when it contains working behavior.
- No `ensure_session` / implicit session creation. Sessions are provisioned by the server via `core.provision_session()`.
- No comments that restate the code. Comments explain non-obvious intent only.
- Inference stays adjacent: no inference dependency enters `woven-core` or `woven-protocol` internals beyond the additive wire message kinds already there. Disabling the plane must never change relay behavior.

---

## `woven-core` public API

### IDs (`crate::ids`)

All are `struct Foo(u64)` with `::new(u64)`, `.get() -> u64`, `From<u64>`, `Display`.

```
NamespaceId  SessionId  SpaceId  EntityId
ConnectionId  PrincipalId  ChannelId  SpaceEpoch
SessionKey { namespace: NamespaceId, session: SessionId }
SpaceKey   { session: SessionKey, space: SpaceId }
```

### Authorization (`crate::auth`)

```rust
// Build grants for a principal
let mut grants = AuthorizationGrants::new();
grants.grant_namespace(ns, AccessGrant::ReadWrite);
grants.grant_session(session_key, AccessGrant::ReadWrite);
grants.grant_space(space_key, AccessGrant::ReadWrite);
grants.grant_channel(ChannelScope::new(session_key, channel_id), AccessGrant::ReadWrite);
// AccessGrant variants: Read, Write, ReadWrite

let principal = AuthenticatedPrincipal::new(PrincipalId::new(1), grants);

// DevAuthenticator — development only, bounded map of token → principal
let mut auth = DevAuthenticator::new();    // DEFAULT_MAX_IDENTITIES = 64
auth.insert("token", principal)?;          // returns Err(CapacityReached) when full
// Implements Authenticator trait
```

### Channel definition (`crate::authority`)

```rust
// Delivery class and persistence are SERVER-CONTROLLED per channel.
// Clients cannot override them; the core enforces ChannelPolicyMismatch.
ChannelDefinition::relay_owned(id, DeliveryClass::ReliableOrdered, PersistenceClass::Ephemeral, max_bytes)
ChannelDefinition::new(id, delivery, persistence, max_bytes)         // same as relay_owned
ChannelDefinition::with_authority(id, delivery, persistence, max_bytes, Arc<dyn AuthorityPolicy>)

// AuthorityPolicy trait — custom validation/transform/emit
// Built-in: RelayOwned (checks session member, space subscriber, entity owner)
// AuthorityOutcome: Accept | Reject(AuthorityRejection) | Transform(AuthorityTransform) | Emit(Box<[AuthorityEmission]>)
```

### Model types (`crate::model`)

```rust
DeliveryClass:    ReliableOrdered | ReliableUnordered | LatestValue | UnreliableSequenced | BestEffortEvent
PersistenceClass: Ephemeral | Stateful | Durable
RoutingPolicy:    BroadcastAll | SpatialGrid2D{cell_size,interest_radius,exact_distance} | SpatialGrid3D{...} | TopicOnly
CoordinateFrame:  Logical | Cartesian2D{meters_per_unit} | Cartesian3D{meters_per_unit}
EntityPosition:   Cartesian2D{x,y} | Cartesian3D{x,y,z}  // finite; must match a spatial space frame

SpaceDescriptor { id, local_frame, parent: Option<ParentAnchor>, epoch, routing }
ParentAnchor    { parent_space: SpaceId, anchor_entity: EntityId }

OutboundMessage { namespace, session, space, space_epoch, entity: Option<EntityId>,
                  channel, sequence, delivery, persistence, coalesce_key: Option<CoalesceKey>, payload: Vec<u8> }
// outbound_message.scoped_coalesce_key() → Option<ScopedCoalesceKey>  (namespace+session+space+epoch+application)

CoalesceKey { channel, entity: Option<EntityId>, component: u64 }
// LatestValue/UnreliableSequenced MUST supply a CoalesceKey; coalescing is fully-scoped by ScopedCoalesceKey

SessionSnapshot { key, member_count, subscription_count, state_bytes, spaces, entities, state }
```

### Queue (`crate::queue`)

```rust
OutboundQueueConfig { total_capacity: 512, critical_capacity: 256, latest_capacity: 512, best_effort_capacity: 128 }
// critical_capacity + latest_capacity + best_effort_capacity may exceed total_capacity (total is the hard ceiling)
// Overflow priority: evict oldest latest → evict best-effort → CriticalCapacityExhausted → disconnect

QueuePush: Queued | QueuedCriticalAfterEviction(QueueEviction) | ReplacedLatest |
           EvictedLatest{key} | EvictedBestEffortForLatest | DroppedLatest |
           DroppedBestEffort | CriticalCapacityExhausted
// CriticalCapacityExhausted → core immediately calls transport_lost on that connection
```

### Core (`crate::core`)

```rust
// Construction
let core = WovenCore::new(authenticator, CoreConfig::default())?;

// CoreConfig defaults (all adjustable):
//   max_connections: 4_096        max_sessions: 1_024
//   max_channels: 1_024           max_memberships_per_connection: 32
//   max_subscriptions_per_connection: 128   max_owned_entities_per_connection: 256
//   max_payload_bytes: 64KB       max_spaces_per_session: 1_024
//   max_space_epoch_tombstones_per_session: 4_096
//   max_entities_per_session: 16_384      max_state_entries_per_session: 65_536
//   max_state_bytes_per_session: 64MB     max_sequence_keys_per_session: 131_072
//   max_authority_emissions: 16   journal_outbox_capacity: 1_024
//   publish_rate_limit: { max_publishes: 256, window: 1s }

// Server setup (before any connections)
core.register_channel(channel_definition)?;   // must be nonzero id, nonzero payload limit
core.provision_session(session_key)?;         // server provisions sessions; clients cannot create them
core.install_space(session_key, descriptor)?; // parent anchor must already exist

// Per-connection lifecycle (call in this order)
let conn_id = core.transport_connected()?;
let principal_id = core.authenticate(conn_id, &Credentials::new("token"))?;
core.join_session(conn_id, session_key)?;     // checks grants, session must already exist
core.subscribe(conn_id, space_key)?;          // checks grants, session membership, space existence
let entity_id = core.spawn_entity(conn_id, space_key, epoch)?;  // server-assigns ID
let outcome = core.publish(publish_request)?; // or core.publish_at(request, Instant)
let summary = core.unsubscribe(conn_id, space_key)?;            // purges queued msgs for that space
let summary = core.leave_session(conn_id, session_key)?;        // purges queued session msgs
core.remove_entity(conn_id, session_key, entity_id)?;
let messages = core.drain_outbound(conn_id)?;
let snapshot = core.snapshot(conn_id, session_key)?;            // scoped to subscribed spaces + grants
let summary = core.transport_lost(conn_id)?;                    // full cleanup, call on disconnect

// Space management
core.advance_space_epoch(session_key, space_id, new_epoch)?;   // evicts entities+state+sequences
core.update_entity_position(conn_id, session_key, entity_id, position)?; // owner-only; updates grid index on cell crossing
// Epoch tombstones prevent ID reuse. Recreation must advance the epoch.

// PublishRequest fields:
//   connection, session, space, space_epoch, entity: Option<EntityId>, channel,
//   sequence (must be strictly monotone per connection+space+epoch+entity+channel+component),
//   delivery (must match ChannelDefinition), persistence (must match ChannelDefinition),
//   coalesce_key (required for LatestValue/UnreliableSequenced), payload

// Introspection helpers
core.is_connected(conn_id) → bool
core.subscription_count(conn_id) → Option<usize>
core.owned_entity_count(conn_id) → Option<usize>
core.sequence_key_count(session_key) → Option<usize>
core.space_epoch_tombstone_count(session_key) → Option<usize>
core.session_count() → usize
core.connection_count() → usize
core.journal_outbox_len() → usize
core.pop_journal_record() → Option<JournalRecord>

// JournalSink trait — async, currently no-op
// NoopJournalSink implements it with a Ready<Ok> future (no runtime needed)
```

### Worker harness (`crate::worker`)

```rust
// Thin synchronous wrapper used by tests and by every transport adapter
let worker = TransportIndependentWorker::new(core);
let mut harness = WorkerHarness::new(worker, capacity)?;  // capacity = max pending commands
harness.submit(Command::TransportConnected)?;
harness.step() → Option<Result<CommandResult, CoreError>>
harness.run_pending() → Vec<Result<CommandResult, CoreError>>

// Commands: TransportConnected | Authenticate{..} | JoinSession{..} | LeaveSession{..}
//           Subscribe{..} | Unsubscribe{..} | SpawnEntity{..} | RemoveEntity{..}
//           UpdateEntityPosition{..} | TransitionEntity(..) | Publish(PublishRequest)
//           Snapshot{..} | DrainOutbound{..} | TransportLost{..}
// CommandResult: Connected(ConnectionId) | Authenticated(PrincipalId) | Joined | Left(CleanupSummary)
//               Subscribed | Unsubscribed(CleanupSummary) | EntitySpawned(EntityId)
//               EntityRemoved(CleanupSummary) | Published(PublishOutcome) | Snapshot(SessionSnapshot)
//               Outbound(Vec<OutboundMessage>) | Disconnected(CleanupSummary)
```

---

## Secure remote QUIC composition and native client

- Default `serve(ServerConfig)` / `serve_dev_ephemeral` remain local development;
  `serve` rejects non-loopback development listeners.
- `woven_server::RemoteServerConfig` has explicit `quic_bind_address`, loopback-only
  `management_bind_address`, and PEM `certificate_file`, `private_key_file`,
  `auth_token_file` paths. `from_env()` requires `WOVEN_REMOTE_QUIC=1` plus
  `WOVEN_QUIC_BIND`, `WOVEN_MANAGEMENT_BIND`, `WOVEN_TLS_CERT_FILE`,
  `WOVEN_TLS_KEY_FILE`, `WOVEN_AUTH_TOKEN_FILE`; partial configuration fails closed.
- `start_remote(config).await -> Result<RemoteServer, ServerError>` exposes actual
  `quic_address`/`management_address`; retain handle, drop to close listeners.
  `serve_remote(config).await` runs until Ctrl-C or listener failure.
- Remote mode explicitly provisions namespace/session 1, logical broadcast spaces 1/2,
  epoch 1, channel 1 ReliableOrdered/Ephemeral and channel 2 LatestValue/Stateful (no TTL),
  64 KiB channel payload ceilings. One externally supplied static credential maps to
  principal 1 through existing `DevAuthenticator` / WVN1 `Development` auth. No default
  or AI token, remote WebTransport, inference, or production tenant authentication.
- `woven_client::ClientTlsConfig::from_ca_pem(&[u8]) -> Result<Self, ClientError>`
  accepts a bounded custom CA PEM bundle; `with_root_certificates(rustls::RootCertStore)`
  accepts a nonempty application-supplied root store. `Client::connect_with_tls(config,
  tls).await` uses normal chain/validity/URL host verification, DNS/IPv4/IPv6, native QUIC
  only, ten-second whole-handshake deadline. No remote insecure fallback. Existing
  `ClientConfig` fields are unchanged; `Client::connect` is literal-loopback-only dev TLS.
- Management HTTP is unauthenticated and must not be exposed or publicly proxied.
  Unix key/token files must deny group/other permissions. Never log credentials.
  See crate READMEs for exact APIs, secure file handling and deliberate limitations.
- Local integration coverage: `woven-server/tests/remote_quic.rs`. Cloud deployment,
  firewall/IAM/secret operations and remote traffic remain separately approval-gated.

## `woven-transport` public API

Shared by every transport adapter (QUIC, WebTransport) and by the inference
coordinator, which uses it exactly like a transport does.

```rust
// Cloneable handle to the single bounded core-worker task
let worker: WorkerHandle = spawn_worker(TransportIndependentWorker::new(core));
worker.execute(command) → Result<CommandResult, TransportError>
worker.register_lifecycle(connection, write_sender, shutdown_sender) → Result<(), TransportError>
worker.subscribe_and_spawn(connection, space, epoch) → Result<EntityId, TransportError>
worker.activate_subscription(connection, space) → Result<(), TransportError>
worker.discard_and_disconnect(connection)   // drains then transport_lost, ignores errors

// Deliver an envelope outside the normal per-connection OutboundQueue, reusing the same
// fan-out plumbing EntityEntered/EntityLeft/SpaceTransition already use:
worker.broadcast_to_space(space, envelope, exclude: Option<ConnectionId>) → Result<(), TransportError>
worker.send_to_connection(connection, envelope) → Result<(), TransportError>

// TransportError: WorkerUnavailable | Core(CoreError) | UnknownConnection

// Shared envelope bridge used by every adapter's post-authentication loop
handle_authenticated(&worker, connection, envelope, &write_sender, inference_sink: Option<&mpsc::Sender<UnroutedControl>>) → Result<(), ()>
// Forwards InferenceRequested/InferenceCancelled to inference_sink when Some; otherwise
// falls through to the normal UnsupportedMessage rejection. None when inference is disabled.
flush_outbound(&worker, connection, &write_sender) → Result<(), ()>
outbound_envelope(message: OutboundMessage) → Envelope
send_envelope(&write_sender, envelope) → Result<(), ()>
send_error(&write_sender, related_kind, code, message)

// A control envelope this crate doesn't itself route, handed to an optional adjacent
// plane instead of rejected. woven-transport has no knowledge of what consumes it.
struct UnroutedControl { connection: ConnectionId, envelope: Envelope }
```

---

## `woven-protocol` public API

```rust
// Constants
PROTOCOL_VERSION: u16 = 1
FILE_IDENTIFIER: &str  = "WVN1"

// Codec — size-prefixed FlatBuffers framing
let codec = Codec::default();                                      // default limits: 1MB frame, 256KB payload
let codec = Codec::new(CodecLimits::new(max_frame, max_payload)?)?;
codec.encode(&envelope) → Result<Vec<u8>, CodecError>
codec.decode(frame: &[u8]) → Result<Envelope, CodecError>
codec.expected_frame_len(prefix: &[u8]) → Result<Option<usize>, CodecError>  // for stream transports
// decode always runs FlatBuffers verifier + semantic validation before returning

// Envelope (owned)
struct Envelope {
    protocol_version: u16,          // must be 1
    delivery_class: DeliveryClass,  // never Unknown
    namespace_id: u64,
    session_id: u64,
    space_id: u64,
    channel_id: Option<u64>,        // required for channel-bearing messages
    entity_id: Option<u64>,
    space_epoch: u64,
    server_tick: u64,
    sender_sequence: u64,
    correlation_id: Option<u64>,
    message: MessagePayload,
}
// Constructors: Envelope::control(delivery, ControlPayload) | ::entity_state | ::reliable_event | ::snapshot

// MessagePayload variants
Control(ControlPayload)           // typed control messages
EntityState(OpaquePayload)        // requires channel_id + entity_id + nonzero type_id, delivery=LatestValue or UnreliableSequenced
ReliableEvent(OpaquePayload)      // requires channel_id + nonzero type_id, delivery=ReliableOrdered or ReliableUnordered
Snapshot(OpaquePayload)           // requires channel_id + nonzero type_id

// OpaquePayload { type_id: u64, bytes: Vec<u8> }  — domain bytes, opaque to routing

// ControlPayload variants (message kind numeric value in parens)
Hello(Hello)                    (1)   — unscoped, delivery=ReliableOrdered
Capabilities(Capabilities)      (2)   — unscoped, delivery=ReliableOrdered
Authenticate(Authenticate)      (3)   — unscoped, delivery=ReliableOrdered
Authenticated(Authenticated)    (4)   — unscoped, delivery=ReliableOrdered
JoinSession(JoinSession)        (5)   — session-scoped, delivery=ReliableOrdered
LeaveSession(LeaveSession)      (6)   — session-scoped, delivery=ReliableOrdered
SubscribeSpace(SubscribeSpace)  (7)   — space+channel-scoped, delivery=ReliableOrdered
UnsubscribeSpace(Unsubscribe)   (8)   — space+channel-scoped, delivery=ReliableOrdered
SubscriptionAccepted(..)        (9)   — space+channel-scoped, delivery=ReliableOrdered
SubscriptionRejected(..)        (10)  — space+channel-scoped, delivery=ReliableOrdered
EntityEntered(..)               (11)  — space+entity-scoped, delivery=ReliableOrdered
EntityLeft(..)                  (12)  — space+entity-scoped, delivery=ReliableOrdered
SnapshotRequest(..)             (15)  — space+channel-scoped, delivery=ReliableOrdered
Snapshot                        (16)  — opaque, space+channel-scoped
SpaceTransition(..)             (17)  — space+entity-scoped, delivery=ReliableOrdered
Ping(Ping)                      (18)  — unscoped, delivery=ReliableUnordered
Pong(Pong)                      (19)  — unscoped, delivery=ReliableUnordered
ProtocolError(..)               (20)  — optional scope, delivery=ReliableOrdered
InferenceRequested(..)          (21)  — space+entity-scoped, delivery=ReliableOrdered, client→server
InferenceAccepted(..)           (22)  — space+entity-scoped, delivery=ReliableOrdered, server→client
InferenceProgress(..)           (23)  — space+entity-scoped, delivery=BestEffortEvent, server→client
InferenceStreamChunk(..)        (24)  — space+entity-scoped, delivery=BestEffortEvent, server→client
InferenceCompleted(..)          (25)  — space+entity-scoped, delivery=ReliableOrdered, server→client
InferenceFailed(..)             (26)  — space+entity-scoped, delivery=ReliableOrdered, server→client
InferenceCancelled(..)          (27)  — space+entity-scoped, delivery=ReliableOrdered, either direction
InferenceExpired(..)            (28)  — space+entity-scoped, delivery=ReliableOrdered, server→client
ToolCallProposed(..)            (29)  — space+entity-scoped, delivery=ReliableOrdered, broadcast to space
ToolCallAccepted(..)            (30)  — space+entity-scoped, delivery=ReliableOrdered, broadcast to space
ToolCallRejected(..)            (31)  — space+entity-scoped, delivery=ReliableOrdered, broadcast to space
ToolCallCompleted(..)           (32)  — space+entity-scoped, delivery=ReliableOrdered, broadcast to space

// Semantic constraints enforced on both encode and decode:
// - Unscoped controls: namespace/session/space/channel/entity/epoch must all be 0/None
// - Session controls: namespace+session nonzero; space/channel/entity/epoch must be absent
// - Space controls: namespace+session+space+epoch nonzero
// - Channel-bearing controls: channel_id required (nonzero)
// - Entity-bearing controls: entity_id required (nonzero)
// - Hello: min_version ≤ max_version, range includes v1, frame/payload limits nonzero
// - Authenticate: scheme != Unknown, credentials nonempty
// - Authenticated: principal_id nonzero
// - DeliveryClass must be compatible with MessageKind (see semantics.rs)

// CodecError variants (for ProtocolError mapping):
// InvalidLimits | FrameTooLarge | PayloadTooLarge | TruncatedFrame | TrailingBytes
// InvalidSizePrefix | InvalidFileIdentifier | InvalidFlatbuffer | UnsupportedProtocolVersion
// UnknownMessageKind | UnknownDeliveryClass | UnsupportedEnumValue | MessageControlMismatch
// MissingPayloadType | UnexpectedDomainPayload | InvalidSemantics{message_kind, reason}

// Schema: crates/woven-protocol/schemas/woven_v1.fbs
// Golden fixtures: crates/woven-protocol/tests/fixtures/{reliable_event_v1,tool_call_completed_v1}.swp
// Companions: crates/woven-protocol/tests/fixtures/*.expected.txt
// FlatBuffers generation: vendored via flatc-fork=0.6.0 + flatbuffers-build=0.2.4 (no system flatc needed)
```

---

## `woven-inference-*` public API

Optional, adjacent plane (ADR 0009). Disabled by default (`ServerConfig::inference_enabled = false`).
Adds no dependency to `woven-core` or `woven-protocol` beyond the wire message
kinds above. An AI identity is an ordinary authenticated core connection — no special
authorization concept exists for it.

```rust
// woven-inference-core: capability/request/provider data model
struct Capability(String);                          // e.g. "language.dialogue"
struct ProviderDescriptor { capability, locality, privacy, modalities, supports_streaming,
                             max_context_items, max_concurrency, latency_class, cost_class, quality_tier }
struct Cancellation;                                 // cheap-clone cooperative cancel flag
struct ContextItem { source: String, bytes: Vec<u8> }
struct InferenceRequest { capability, principal, acting_entity, deadline: Instant,
                           cancellation, context: Vec<ContextItem>, input: Vec<u8>, streaming }
struct ToolCallProposal { tool_id, tool_version, arguments: Vec<u8>, expected_revision: u64 }
enum InferenceEvent { Progress{percent} | StreamChunk{sequence,chunk,is_final} |
                       ToolCallProposed(ToolCallProposal) | Completed{result} | Failed{reason} }
struct InferenceOutcome { events: Vec<InferenceEvent> }

#[async_trait]
trait Provider: Send + Sync {
    fn descriptor(&self) -> ProviderDescriptor;
    async fn run(&self, request: InferenceRequest) -> InferenceOutcome;
}

// woven-inference-tools: bounded registry + deterministic gateway (ADR 0010)
struct ToolDefinition { id, version, side_effect: SideEffect }  // SideEffect: ReadOnly | StateChanging
struct ToolInvocationContext { worker: WorkerHandle, connection, entity, space, space_epoch }
enum ToolCallOutcome { Completed{new_revision, result} | Rejected{code: ToolCallRejectionReason, reason} }

#[async_trait]
trait ToolHandler: Send + Sync {
    fn definition(&self) -> ToolDefinition;
    async fn invoke(&self, context: &ToolInvocationContext, proposal: &ToolCallProposal) -> ToolCallOutcome;
}

let mut registry = ToolRegistry::new();              // bounded, MAX_REGISTERED_TOOLS = 64
registry.register(Arc<dyn ToolHandler>) → Result<(), ToolRegistryError>
registry.evaluate(&context, &proposal).await → ToolCallOutcome
// A state-changing handler is responsible for its own staleness check (compare
// proposal.expected_revision against its own counter) before calling core.publish() —
// there is no core-level revision primitive; see demo::StatusUpdateTool for the pattern.

// woven-inference-test-provider: deterministic scripted Provider, no network/randomness
DeterministicProvider;                                // implements Provider
// demo::{DiagnosticTool, StatusUpdateTool} in woven-inference-tools pair with its scripts

// woven-inference-coordinator: runs one AI identity, drives providers/tools
struct AiIdentityConfig { token, namespace, session, space, space_epoch, status_channel }
struct CoordinatorConfig { worker, identity, provider: Arc<dyn Provider>, tools: Arc<ToolRegistry>, queue_capacity }
spawn(config, inbound: mpsc::Receiver<UnroutedControl>) → Result<(ConnectionId, EntityId), CoordinatorError>
// Establishes the identity's core connection (TransportConnected → Authenticate → JoinSession
// → subscribe_and_spawn), then runs its bounded request queue (tokio::sync::Semaphore) and
// drain-poll task in the background. Rejects with InferenceFailed when the queue is full,
// rather than queuing unboundedly.
```

---

## Architecture decisions (summaries — read full ADR only if extending that area)

| ADR | Decision |
|---|---|
| 0001 | Core is transport-independent. Networking, routing, authority, persistence, and inference are explicit separate layers |
| 0002 | QUIC/WebTransport instead of raw UDP. No application-level encryption, congestion control, or fragmentation |
| 0003 | Binary WebSocket was the universal baseline. **Superseded by ADR 0014** — WebSocket was removed entirely |
| 0004 | Session owns a graph of spaces. Every space has local coordinates anchored to a parent entity. Cross-space transitions are sequenced and epoch-protected |
| 0005 | FlatBuffers with pinned versioned schema. Additive-only evolution. Never reuse IDs. Golden fixtures prove cross-language equivalence |
| 0006 | Bounded queues everywhere. Priority: evict replaceable → drop best-effort → disconnect slow consumer |
| 0007 | Authority is per-channel, not global. Default RelayOwned. Custom policies via trait |
| 0008 | Persistence seam outside the realtime hot path. In-memory + no-op journal now; pluggable later |
| 0009 | Inference is an optional adjacent plane. Never inside room-worker hot loops. Disabling it must leave relay tests unchanged |
| 0010 | Model output passes through a deterministic tool gateway. Models never mutate state directly |
| 0011 | Compute Engine VM + external passthrough NLB for staging. Not Cloud Run (wrong lifecycle). Not GKE until scale justifies it |
| 0012 | Normal target $10–30/month. No GPU continuously. All cloud mutations approval-gated |
| 0013 | Deferred complexity list: distributed consensus, GKE, GPU inference, vector DBs, agent frameworks, UDP, app-level fragmentation |
| 0014 | QUIC/WebTransport only. WebSocket removed as a transport entirely; native clients use QUIC, browsers use WebTransport. Standardized URI is `quic://host:port`; browsers derive WebTransport via the deterministic port convention (WebTransport one port above QUIC, on `/webtransport`) |

ADRs 0011/0012 record decisions made for this repo's own (deferred) cloud staging work.
Cloud orchestration is now expected to live in a separate consuming project; treat these two
as historical context rather than an active plan for this repo.

---

## Approval gates (require explicit user approval before proceeding)

- Creating or modifying billable cloud resources
- Allocating or resizing GPUs
- Expanding IAM permissions
- Creating or rotating secrets
- Changing DNS or public exposure
- Deploying to staging or production
- Any action that materially increases expected monthly cost

---

## Environment and toolchain

```
Rust: 1.98.0 stable (rust-toolchain.toml pins this)
Cargo workspace resolver: 3, edition: 2024, rust-version: 1.88
Available: cargo, rustfmt, clippy, Node 22.23.2, npm 10.9.8, .NET SDK 10.0.111, Python 3.12.3, Docker 29.7.2, gh 2.98.0
Absent: system flatc (vendored via Cargo), Terraform, OpenTofu
GitHub: jdharrison/woven (public), SSH remote, gh authenticated
CI: .github/workflows/ci.yml — Rust (format/clippy/test/doc), dependency audit, TypeScript client
```

Workspace lints (inherited by all crates via `[lints] workspace = true`):
- `unsafe_code = "deny"` (core overrides to `forbid`)
- `clippy::all`, `clippy::pedantic` at warn; `missing_errors_doc` and `module_name_repetitions` allowed

---

## Engineering constraints (still apply going forward)

- Do not introduce GKE, Cloud Run, Redis, vector DBs, or distributed consensus at any point without explicit ADR revision and user approval.
- Benchmark before introducing unsafe code, lock-free structures, or custom allocators.
- No container, Dockerfile, or cloud infrastructure exists in this repo yet; that work (if it happens here at all) is approval-gated per the hard rules above.
