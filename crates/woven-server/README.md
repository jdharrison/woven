# woven-server

Self-hosted QUIC and WebTransport realtime server for Woven.

```sh
cargo install woven-server
woven-server
```

See the repository README for local development and release-binary guidance.

## Opt-in managed QUIC and WebTransport

`ManagedServerConfig`, `start_managed`, and `serve_managed` provide a separate managed
composition. It starts with no client credentials or sessions. Native QUIC remains mandatory.
The binary requires all of `WOVEN_MANAGED_QUIC=1`, `WOVEN_QUIC_BIND`,
`WOVEN_MANAGEMENT_BIND`, `WOVEN_ADMIN_BIND`, `WOVEN_TLS_CERT_FILE`,
`WOVEN_TLS_KEY_FILE`, and `WOVEN_ADMIN_TOKEN_FILE`. Do not combine these with
`WOVEN_REMOTE_QUIC` or `WOVEN_AUTH_TOKEN_FILE`. Both HTTP listeners must bind loopback;
TLS and private secret-file rules below also apply. Unix files are opened without following
symlinks.

Managed WebTransport is optional and all-or-nothing. To enable it, also set:

```sh
WOVEN_MANAGED_WEBTRANSPORT=1
WOVEN_WEBTRANSPORT_BIND=0.0.0.0:8082
WOVEN_WEBTRANSPORT_PATH=/webtransport
WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS=https://app.example.com,https://admin.example.com
```

The bind address and request path are explicit; there is no QUIC-plus-one port convention in
managed mode. The entire comma-separated origin value is limited to 2,048 bytes and contains
1–64 exact canonical HTTP(S) URL origins. Canonical values use lowercase schemes/hosts, omit
HTTP/HTTPS default ports, and contain no trailing slash, path, query, fragment, credentials, or
whitespace. Duplicates are rejected. Requests with a missing `Origin` are rejected, as are exact
scheme, host, or port mismatches. The path is at most 256 bytes and must be an absolute path
without a query or fragment. Supplying only part of this group, setting an enable flag other than
exactly `1`, reusing the native QUIC UDP address, or supplying invalid values fails startup
closed. When the group is absent, no managed WebTransport socket or task is created and
capabilities continue to report QUIC only.

Both managed transports use the exact configured PEM certificate chain and matching private key.
All files are validated before serving starts. Managed WebTransport never generates or falls back
to a self-signed identity; certificate issuance, trust, browser compatibility, and renewal remain
operator responsibilities.

Point Host's `WOVEN_MANAGEMENT_URL` at the **admin** listener (`WOVEN_ADMIN_BIND`),
not the read-only `WOVEN_MANAGEMENT_BIND` listener. Host must use the matching
independent admin credential through its management-token file.

The independent admin listener requires a Bearer admin credential on every route:

- `GET /v1/node`
- `PUT`, `GET`, `PATCH`, `DELETE /v1/namespaces/{namespaceId}/sessions/{sessionId}`
- `PUT /v1/namespaces/{namespaceId}/sessions/{sessionId}/spaces/{spaceId}`

Mutations require the current `Woven-Node-Incarnation` header. PUT accepts
`revision` (canonical nonzero decimal string), `allocatedCCU` (0–4096), and a
Host-supplied 64-lowercase-hex `clientToken`. PUT and PATCH additionally accept optional
`tickRateHz` (integer 1–120): a publish budget per connected session member per second,
aggregated across channels/spaces, **not** a simulation scheduler. PUT omission keeps legacy
session-unlimited behavior (the core connection limit still applies); PATCH omission preserves
the configured ceiling. Snapshots omit `tickRateHz` unless configured. Rate changes/retries do
not reset connection/session windows, including across LeaveSession/re-admission/resubscription;
same-revision retries cannot change the requested rate or its presence. Bounded histories share
the core membership cap (32 by default), prune expired detached windows before joining, and
reject a new session rather than evict an unexpired budget. Never-limited sessions drop history
on leave; disconnect and managed deletion reclaim it. PATCH otherwise accepts only revision/capacity;
DELETE requires quoted current-revision `If-Match`. Authenticated `GET /v1/node` includes
`"transports": { "quic": true, "webTransport": { "enabled": <boolean>,
"certificateSha256"?: "<lowercase-hex>" } }`. The hash is SHA-256 over the DER bytes of the
actual first certificate in the configured WebTransport TLS chain and is present only when that
listener is enabled. This strict object contains no endpoint address or URL; Host supplies the
externally reachable endpoint separately. Responses match Host's monitoring contract and never
return tokens or verifiers. The separate read-only management listener does not expose these
routes or standalone HTTP admission routes.

Managed Lite exposes spaces 1/2 at epoch 1 with exactly `channelIds: ["1", "4"]`.
Channel 1 remains ReliableOrdered/Ephemeral; channel 4 is generic opaque
UnreliableSequenced/Ephemeral state. Both have a 64 KiB payload ceiling. No Stateful
channel is registered or granted. The authenticated `/v1/node` channel definitions
and scoped grants match this contract. Host consumers of the former one-channel
contract must update their exact schema and descriptor channel IDs.

Managed sessions may add at most 64 spatial subspaces (66 spaces total including system scopes
1/2) with authenticated add-only `PUT
/v1/namespaces/{namespaceId}/sessions/{sessionId}/spaces/{spaceId}`. The strict body is:

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

The node fixes Cartesian3D/SpatialGrid3D, epoch 1, no parent, and channels 1/4. Scale, cell size,
and radius must be finite and positive; bounds are finite with strict min < max and inclusive at
runtime. The endpoint shares the session revision, returns `201` for an add and `200` for an exact
same-revision retry, and never updates/deletes an existing definition. It atomically updates live
and future exact grants. Unknown/ad-hoc spaces remain unauthorized; there is no wildcard grant.
Session snapshots expose complete definitions. `/v1/node` advertises the positioned-state and
managed-spatial capabilities plus 64-additional/66-total limits.

The core worker atomically installs scoped grants, spaces and mandatory admission.
Tokens are retained only as SHA-256 verifiers; each authenticated connection gets a
unique principal. DELETE retires the verifier/scope and closes admitted, waiting and
not-yet-joined connections. Capacity decreases drain without kicking existing members.
History is in-memory and bounded: reconciliation after restart needs a new incarnation
binding and fresh credentials. Managed QUIC and WebTransport both require the WVN1
`AuthenticationScheme::Bearer` label; a valid managed token sent as `Development` is rejected.
Development and static remote compositions retain their existing Development-scheme defaults.
This is possession-based session access, not per-user identity, persistence, federation, or a
production-hardening claim.

`ManagedServer` exposes actual `quic_address`, optional `webtransport_address` and
`webtransport_url`, `management_address`, `admin_address`, `node_incarnation`, and a trusted
`worker` handle. The WebTransport URL uses `https://` and the configured path. Retain the server
while serving; drop closes both transport endpoints and aborts all listener tasks.
`worker.manage(ManagedRequest)` is the trusted local API. `RequestSessionAdmission` already
admits and joins atomically; no second worker join command is required. Queue operations remain
worker-serialized and connection-owned. The read-only `/v1/capabilities` response includes
`webtransport` in `transports` and the actual local `port/path` only when the listener started.

See [the complete managed contract](../../docs/managed-sessions.md) for API limits and
admission semantics. Both listeners use the same managed `WorkerHandle` and WVN1 bridge.
`tests/managed_quic.rs` preserves real TLS-verified native QUIC admission, queueing, scope
isolation, and teardown coverage. `tests/managed_webtransport.rs` adds real TLS-verified
WebTransport coverage for Bearer authentication, Development-scheme rejection, direct admission,
queue claim, atomic admission/join followed by subscription, wrong token/scope, exact-origin
rejection, leaf-fingerprint metadata, scope deletion, and server-drop teardown. The TypeScript
WebTransport client exposes the same bounded admission/queue operations and cancellation helper;
its coverage includes a mock WHATWG transport, Rust wire-compatibility fixtures, and a bounded
headless-Chromium E2E connected to a disposable managed listener.

The cross-repository `tests/host_managed_local.rs` E2E has also passed via
`npm run test:local` from the sibling `../woven-host` checkout (relative to Woven's root).
It runs the real Host HTTP API against isolated Firebase Auth/Firestore emulators, provisions
this node through authenticated admin HTTP, and uses Host-returned descriptors with the public
native QUIC client. Coverage includes capacity/monitoring, queue/claim, TLS/scope rejection,
server deletion and account teardown. It is ignored by ordinary Cargo runs and does not validate
cloud deployment, production Firebase/App Check, browser UI, or Host-provided WebTransport
descriptors.

### Managed embedding API

```rust,ignore
pub struct ManagedWebTransportConfig {
    pub bind_address: std::net::SocketAddr,
    pub path: String,
    pub allowed_origins: Vec<String>,
}

pub struct ManagedServerConfig {
    // Existing native QUIC, HTTP, TLS, and admin credential fields remain required.
    pub webtransport: Option<ManagedWebTransportConfig>,
}

pub struct ManagedServer {
    pub quic_address: std::net::SocketAddr,
    pub webtransport_address: Option<std::net::SocketAddr>,
    pub webtransport_url: Option<String>,
    // Existing management/admin/incarnation/worker fields remain available.
}
```

An embedding Host that constructs `ManagedServerConfig` must now set `webtransport: None` to
preserve native-QUIC-only behavior, or provide the complete nested configuration. If Host exposes
a browser connection descriptor, it must consume an explicitly configured externally reachable
WebTransport URL; the returned `webtransport_url` is actual local listener metadata and must not
be relabeled as a public URL behind NAT, ingress, or port mapping.

## Local node/client log capture and Host feed

The shared worker captures `ClientLog` (WVN1 kind 40, capability mask `CAPABILITY_CLIENT_LOG = 2`)
locally, never broadcasts logs to peers, and never writes them to a database, cloud
service, or disk. Messages contain 1–1,024 UTF-8 bytes and an Info/Warn/Error level.
Authorization requires the **actual authenticated joined session**, not merely a
credential grant or an asserted namespace/session. The worker attaches connection ID,
namespace/session, capture timestamp, and node-global sequence. Client logs are limited
to 10 per connection per rolling second and 1,024 aggregate per rolling second;
rejections do not consume retained-ring capacity. Do not put credentials or sensitive
application payloads in log messages.

Node entries are limited to `client.connected` on each first successful session
membership (including admission/queue claim) and `client.disconnected` on leave,
transport loss, managed revocation, or internal slow-consumer cleanup. Failed/queued
admissions, duplicate joins/cleanup, and publications/position updates add no entries.
Node messages are fixed server text, never client-supplied leave reasons.

Managed mode exposes **only on the independent authenticated admin listener**:

```text
GET /v1/logs?after=0&limit=32
Authorization: Bearer <independent admin credential>
Woven-Node-Incarnation: <current incarnation>
```

Both query parameters are required. `after` is a canonical `u64` decimal (zero allowed);
`limit` is canonical decimal 1–32. Unknown/duplicate/encoded/noncanonical parameters,
request bodies, and duplicate headers fail closed. Every log read requires the exact
current incarnation; missing/stale/duplicate incarnation returns `409`. Existing admin
concurrency, rate, timeout and `Cache-Control: no-store` safeguards apply. The
unauthenticated read-only management listener does **not** expose this feed.

The Host-facing JSON contract is:

```json
{
  "nodeIncarnation": "current-incarnation",
  "entries": [{
    "sequence": "1",
    "occurredAtMs": 1800000000000,
    "namespaceId": "1",
    "sessionId": "1",
    "connectionId": "1",
    "source": "node",
    "event": "client.connected",
    "level": "info",
    "message": "session joined"
  }],
  "nextSequence": "1",
  "droppedThrough": "0"
}
```

`source` is `client|node`, `event` is
`client.log|client.connected|client.disconnected`, and `level` is `info|warn|error`.
IDs/sequences are decimal strings; timestamps are numeric capture-time milliseconds.
Sequences start at 1 and never wrap within a node incarnation. The global ring retains
at most **2,048** entries across all tenants. `droppedThrough` is the highest evicted
sequence (initially `"0"`). Entries are strictly after the requested cursor;
`nextSequence` is the last **returned** sequence, or `after` when empty. Pages may
contain fewer than `limit` entries: the total serialized JSON is capped at **48 KiB**,
including actual worst-case string escaping, safely below the 65,536-byte collection
ceiling. Use `nextSequence` unchanged for continuation and detect gaps with
`droppedThrough`.

This is a volatile, best-effort buffer pending external Host collection, **not durable
logging** or an ACK of Host receipt. Collection does not consume entries. Slow collection,
eviction, shutdown, or restart can lose logs; a new incarnation requires deliberate
cursor reconciliation. Host owns any persistence/retention outside Woven.

Bridge-level log authorization, rate, scope, and size rejections return `ProtocolError`
without closing a healthy connection. **Current adapter limitation:** an oversized or
otherwise semantically invalid wire `ClientLog` is rejected by the protocol codec before
it reaches the bridge; QUIC/WebTransport currently treat that decode failure as fatal.
Nonfatal oversized-wire rejection requires a separate adapter read-loop change.

## Opt-in static remote native QUIC

The default remains local development: loopback HTTP/QUIC/WebTransport, generated
self-signed TLS, and development credentials. `serve(ServerConfig)` now rejects
non-loopback development listeners. Do not expose that mode through a tunnel or proxy.

For a controlled remote test, the binary supports an explicit separate composition:

```sh
WOVEN_REMOTE_QUIC=1 \
WOVEN_QUIC_BIND=0.0.0.0:8081 \
WOVEN_MANAGEMENT_BIND=127.0.0.1:8080 \
WOVEN_TLS_CERT_FILE=/run/woven/tls/chain.pem \
WOVEN_TLS_KEY_FILE=/run/woven/tls/key.pem \
WOVEN_AUTH_TOKEN_FILE=/run/woven/auth/token \
woven-server
```

This is a configuration example, **not deployment authorization**. None of these
files are included in the repository. Use already approved, operator-provisioned
material; do not put token values in command arguments, URLs, source code, logs,
checked-in environment files, or shell history. Environment variables carry **paths**,
not secret contents. The key/token files must be readable only by the service account
(mode `0600` or `0400` on Unix; group/other permission bits are rejected). Protect
parent directories against replacement; on non-Unix platforms enforce equivalent
ACLs externally. Credential/key bytes are held in process memory, not zeroized.

All six variables are required together. Partial configuration, invalid addresses,
missing/malformed TLS, mismatched keys, empty/short/whitespace credentials, oversized
inputs, and public management binds fail closed before listeners start. The token is
32–4096 non-whitespace ASCII bytes, with an optional trailing newline. Length validation
is not an entropy test: use an independently provisioned cryptographically random
credential of at least 256 bits, encoded as hex/base64. TLS files are capped at 1 MiB;
the credential file is capped at 4098 bytes.

### TLS and private management

- Supply a PEM certificate chain, leaf first, and its matching unencrypted PEM private
  key. Clients must trust its issuing CA (or explicitly trust a test self-signed
  certificate) and verify validity and the URL host against DNS/IP subject alternative
  names. An IP URL requires an **IP SAN**, not a DNS SAN containing an IP string.
- Native QUIC uses UDP at the explicit QUIC bind address. No WebTransport listener is
  opened, even at the usual QUIC-plus-one port. Inference is disabled.
- HTTP `/healthz`, `/readyz`, `/metrics`, and `/v1/capabilities` are **unauthenticated**
  and must remain loopback/private. This is not a public management API. Do not forward
  or expose it publicly. Capabilities report QUIC only and no WebTransport endpoint.
- Startup output contains listener addresses, not credentials or TLS contents. Remote
  mode rejects `--log-all` and `--log-transform`. Do not enable dependency TLS key logging
  or unsafe payload logging in an embedding application.
- Files load once at startup; certificate/credential replacement requires restart.
  There is no automatic certificate issuance, renewal, hot reload, or remote revocation.

### Deliberately fixed scope, not production tenant authentication

The server explicitly provisions namespace **1**, session **1**, logical broadcast
spaces **1 and 2**, and bounded Cartesian3D/SpatialGrid3D space **3**, all at epoch **1**.
Space 3 uses 1 meter/unit, 10-unit cells, a 25-unit exact interest radius, and inclusive
bounds from `(-1000, -100, -1000)` through `(1000, 500, 1000)`. The single supplied
credential authenticates all holders as principal **1**, with read/write grants for that
fixed session, those spaces, and these channels only:

| Channel | Delivery | Persistence | Payload ceiling |
|---|---|---|---|
| 1 | ReliableOrdered | Ephemeral | 64 KiB |
| 2 | LatestValue | Stateful, no TTL | 64 KiB |
| 4 | UnreliableSequenced | Ephemeral | 64 KiB |

Entity ownership remains per connection and server-assigned; channel 2 requires the
normal entity/coalescing-key semantics (`Client::publish_state`). Client frame limits
may impose a smaller practical payload than the channel ceiling. Existing core rate,
capacity, queue, and policy limits apply; no client policy overrides are introduced.

Development uses the same channels 1/2/4 and separately retains AI status channel 3.
Channel 4 carries opaque `EntityState`; the server does not impose an application
payload format. Persistence is resolved from the registered channel via the bounded
worker. UnreliableSequenced frames use datagrams on every outbound path, including immediate
flushes and direct write-queue delivery. Unsupported, oversized, or failed datagram
sends drop without reliable-stream fallback. The 64 KiB payload ceiling is not a
promise that such a payload fits a negotiated datagram budget. Ephemeral state is
not cached or replayed to late joiners. Duplicate/older UnreliableSequenced updates
are rejected without disconnecting; client datagrams carrying control or reliable
traffic are ignored.

QUIC and WebTransport keep one pending reliable-frame read pinned across datagram
processing, so a partial size prefix/body cannot lose framing. Transport subscriptions
activate only after `SubscriptionAccepted` and the client's own `EntityEntered` are
queued. Peer payloads for pending spaces remain in the original bounded outbound queue
until activation; other active spaces continue draining. This adds no initial-roster
message or replay protocol.

This reuses the existing bounded `DevAuthenticator` and WVN1 `Development` authentication
scheme as a **static scoped credential**. It is not OIDC/JWT validation, per-user tenant
identity, hosted entitlement enforcement, mTLS, a token-expiry system, or a hardened
public-service security boundary. Neither `dev-token` nor the AI demo credential is
installed. Any authenticated holder can join the fixed session; applications still
need real identity/admission integration before claiming production tenant isolation.
There is no federation/shared state between nodes, persistence across restart, or
configurable remote provisioning API here. Network abuse/DoS hardening is not supplied
by this composition; existing transport/core bounds do not replace perimeter controls.

### Embedding API

`woven_server` exports:

```rust,ignore
pub struct RemoteServerConfig {
    pub quic_bind_address: std::net::SocketAddr,
    pub management_bind_address: std::net::SocketAddr,
    pub certificate_file: std::path::PathBuf,
    pub private_key_file: std::path::PathBuf,
    pub auth_token_file: std::path::PathBuf,
}

// None only when no remote environment settings are present.
RemoteServerConfig::from_env() -> Result<Option<RemoteServerConfig>, ServerError>;
start_remote(config: RemoteServerConfig) -> Result<RemoteServer, ServerError>; // async
serve_remote(config: RemoteServerConfig) -> Result<(), ServerError>; // async, Ctrl-C
```

`RemoteServer` exposes actual `quic_address` and `management_address` (use port 0 for
local tests). Keep it alive; dropping it closes QUIC and aborts listener tasks. Connection
cleanup follows asynchronously; this is not a drain-and-persist shutdown protocol.
`serve_remote` exits on Ctrl-C or listener failure.

See the [native client README](../woven-client-rust/README.md) for verified TLS usage.
Local coverage lives in `tests/remote_quic.rs`; it uses ephemeral loopback listeners and
throwaway test certificates only. No cloud, firewall, IAM, secret-manager, deployment,
or remote load-test operations are performed by these tests.
