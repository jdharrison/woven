# woven-server

Self-hosted QUIC and WebTransport realtime server for Woven.

```sh
cargo install woven-server
woven-server
```

See the repository README for local development and release-binary guidance.

## Opt-in managed native QUIC

`ManagedServerConfig`, `start_managed`, and `serve_managed` provide a separate managed
composition. It starts with no client credentials or sessions. The binary requires
all of `WOVEN_MANAGED_QUIC=1`, `WOVEN_QUIC_BIND`, `WOVEN_MANAGEMENT_BIND`,
`WOVEN_ADMIN_BIND`, `WOVEN_TLS_CERT_FILE`, `WOVEN_TLS_KEY_FILE`, and
`WOVEN_ADMIN_TOKEN_FILE`. Do not combine these with `WOVEN_REMOTE_QUIC` or
`WOVEN_AUTH_TOKEN_FILE`. Both HTTP listeners must bind loopback; TLS and private
secret-file rules below also apply. Unix files are opened without following symlinks.

Point Host's `WOVEN_MANAGEMENT_URL` at the **admin** listener (`WOVEN_ADMIN_BIND`),
not the read-only `WOVEN_MANAGEMENT_BIND` listener. Host must use the matching
independent admin credential through its management-token file.

The independent admin listener requires a Bearer admin credential on every route:

- `GET /v1/node`
- `PUT`, `GET`, `PATCH`, `DELETE /v1/namespaces/{namespaceId}/sessions/{sessionId}`

Mutations require the current `Woven-Node-Incarnation` header. PUT accepts
`revision` (canonical nonzero decimal string), `allocatedCCU` (0–4096), and a
Host-supplied 64-lowercase-hex `clientToken`. PATCH accepts only revision/capacity;
DELETE requires quoted current-revision `If-Match`. Responses match Host's monitoring
contract and never return tokens or verifiers. The separate read-only management
listener does not expose these routes or standalone HTTP admission routes.

The core worker atomically installs scoped grants, spaces and mandatory admission.
Tokens are retained only as SHA-256 verifiers; each authenticated connection gets a
unique principal. DELETE retires the verifier/scope and closes admitted, waiting and
not-yet-joined connections. Capacity decreases drain without kicking existing members.
History is in-memory and bounded: reconciliation after restart needs a new incarnation
binding and fresh credentials. This is possession-based session access, not per-user
identity, persistence, federation, or a production-hardening claim.

`ManagedServer` exposes actual `quic_address`, `management_address`, `admin_address`,
`node_incarnation`, and a trusted `worker` handle. Retain it while serving; drop initiates
QUIC closure and aborts listeners. `worker.manage(ManagedRequest)` is the trusted local
API. `RequestSessionAdmission` already admits and joins atomically; no second worker
join command is required. Queue operations remain worker-serialized and connection-owned.

See [the complete managed contract](../../docs/managed-sessions.md) for API limits
and native client constraints. The WVN1 bridge and native client integration are
implemented; `tests/managed_quic.rs` covers real TLS-verified QUIC admission, queueing,
scope isolation and teardown. The cross-repository `tests/host_managed_local.rs` E2E
has also passed via `npm run test:local` from the sibling `../woven-host`
checkout (relative to Woven's root). It runs the real Host HTTP API against isolated
Firebase Auth/Firestore emulators, provisions this node through authenticated admin
HTTP, and uses Host-returned descriptors with the public native QUIC client. Coverage
includes capacity/monitoring, queue/claim, TLS/scope rejection, server deletion and
account teardown. It is ignored by ordinary Cargo runs and does not validate cloud
deployment, production Firebase/App Check, browser UI, or WebTransport.

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
spaces **1 and 2**, epoch **1**. The single supplied credential authenticates all
holders as principal **1**, with read/write grants for that fixed session, those spaces,
and these channels only:

| Channel | Delivery | Persistence | Payload ceiling |
|---|---|---|---|
| 1 | ReliableOrdered | Ephemeral | 64 KiB |
| 2 | LatestValue | Stateful, no TTL | 64 KiB |

Entity ownership remains per connection and server-assigned; channel 2 requires the
normal entity/coalescing-key semantics (`Client::publish_state`). Client frame limits
may impose a smaller practical payload than the channel ceiling. Existing core rate,
capacity, queue, and policy limits apply; no client policy overrides are introduced.

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
