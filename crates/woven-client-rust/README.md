# woven-client

Native Rust client for Woven using QUIC and WebTransport.

```sh
cargo add woven-client
```

`woven-client` is a library crate. It does not provide a standalone executable.

## Session logger

After joining an unmanaged session or receiving a verified managed `Admitted`
result, log without repeating scope:

```rust,ignore
client.logger().info("connected").await?;
client.logger().warn("cache miss").await?;
client.logger().error("operation failed").await?;
client.log("info alias").await?;
```

`Client::logger(&mut self) -> ClientLogger<'_>` borrows the ordered control
stream. Its async `info`, `warn`, and `error` methods accept `impl AsRef<str>`
and return `Result<(), ClientError>`; `Client::log` is the info alias. No
logging queue or retries are introduced. Each call sends one session-scoped
`ClientLog`, never a broadcast. **`Ok(())` means sent, not server acceptance,
Host persistence, or a persistence acknowledgement.** Receive control traffic
to observe server rejections, and never put secrets in messages.

Messages must contain 1–1024 UTF-8 bytes (`woven_protocol::MAX_LOG_MESSAGE_BYTES`);
smaller negotiated payload/frame limits still apply. Missing joined/admitted
scope and invalid messages fail locally. Successful request admission, queue
operations returning Admitted (including claim), and the consuming admission
runner automatically remember scope. Queued/Paused/Rejected do not establish it.

Legacy join has no success acknowledgement: scope is remembered after sending
`join_session` and cleared on an observed join rejection. The additive
`leave_session(reason: impl AsRef<str>)` leaves the remembered session and clears
it. Admission abort, stream I/O failure, and observed invalid-session log errors
also clear scope; close consumes the client. A later log requires another
join/admission. Protocol tooling exports `ClientLog { level, message }`,
`LogLevel` (Info=1, Warn=2, Error=3), and `CAPABILITY_CLIENT_LOG = 2` from
`woven-protocol`. If the server did not negotiate `CAPABILITY_CLIENT_LOG`, all
logger methods and `log` reject locally with `ClientError::UnsupportedCapability("ClientLog")`
before writing bytes, while retaining joined scope and leaving the connection usable.

## Managed native admission (wire/client slice)

Host supplies the endpoint, opaque token, namespace/session, allowed spaces/channels,
 and CA trust material. Use `Client::connect_with_tls_and_auth(config, tls,
woven_protocol::AuthenticationScheme::Bearer)` for explicit bearer authentication.
Existing `connect`/`connect_with_tls` and `ClientConfig` fields retain Development
compatibility. No JWT format, discovery, or insecure remote fallback is introduced.

Before subscribing, use `request_admission(namespace, session, correlation, key)`.
It returns sanitized `AdmissionResult`; Admitted means the worker has already joined.
For Queued, use `queue_status`, `queue_heartbeat`, `queue_claim`, or `queue_cancel`,
each taking `(namespace, session, correlation, ticket_id)` and returning `QueueUpdate`.
Each exchange is bounded to ten seconds. Use unique nonzero correlations and reuse
an idempotency key only for the same logical request on the same connection/scope.
These are exclusive pre-subscription exchanges, not a multiplexed application inbox;
unexpected traffic, transport failure, and timeouts fail closed rather than dropping
messages silently or reusing a partially read stream. Do not externally cancel a
borrowed exchange and then reuse the client; drop/close it instead.

`client.admit_with_cancellation(namespace, session, key, timeout, cancellation_future)`
consumes a fresh authenticated client and returns `(Client, ManagedAdmissionOutcome)`
on semantic completion. Timeout must be positive and at most 15 minutes. It sends
heartbeats while waiting and claims observed offers, clamping advisory poll intervals
to 1–5 seconds. Paused/rejected/terminal results are returned without retries. It
performs zero transport retries (within the contract's maximum of three), because
partial stream I/O cannot safely be replayed. Cancellation, deadline, or I/O error
closes/drops the connection, including races with an admitted claim; worker cleanup
must release its ticket/lease. Close initiation is not proof of peer receipt.

`queue_cancel` does not leave an already admitted session; disconnect to release it.
Legacy `join_session` is unchanged and is not a managed admission bypass. Managed
core/transport bridging and real TLS-verified native QUIC integration are implemented
and covered by `woven-server/tests/managed_quic.rs`. The cross-repository
`woven-server/tests/host_managed_local.rs` E2E has also passed via
`npm run test:local` from the sibling `../woven-host` checkout (relative to
Woven's root): real Host HTTP APIs provision the managed node and supply descriptors
used by this native client. It uses isolated Firebase Auth/Firestore emulators and
loopback sockets, not a cloud deployment or browser UI; ordinary Cargo runs ignore it.

## Payload guardrails

The default payload limit is **64 KiB (65,536 bytes) per update**, measured in
serialized bytes, with a separate **1 MiB frame limit** for protocol metadata.
`publish_state`, `publish_positioned_state`, `publish_event`, `publish_unreliable_state`, and
`publish_unreliable_positioned_state` reject larger values before serialization or transport writes
with `ClientError::Protocol(CodecError::PayloadTooLarge)`;
the error includes the actual and maximum byte counts. The connection remains
usable after a local size rejection. A smaller configured or server-advertised
limit also restricts outgoing payloads; raising client receive limits does not
raise the 64 KiB publish ceiling. Individual server channels may impose an even
smaller limit and remain authoritative.

Send granular entity/component deltas rather than a serialized world in one
property/state update. Oversized values are rejected, not truncated. Incoming
frames and payloads are bounded by the limits advertised in `ClientConfig`.

## Positioned entity state

The client advertises `CAPABILITY_POSITIONED_ENTITY_STATE` and retains the intersection returned
by the server. Check `client.supports_positioned_state()`. Both positioned methods fail locally
with `ClientError::UnsupportedCapability("positioned EntityState")` before serialization or I/O
when the capability was not negotiated.

```rust,ignore
use woven_client::RoutingPosition3D;

client.publish_positioned_state(
    namespace_id, session_id, space_id, space_epoch, latest_value_channel_id,
    entity_id, sequence, type_id,
    RoutingPosition3D { x: 9.9, y: 0.0, z: 0.0 },
    state_bytes,
).await?;

client.publish_unreliable_positioned_state(
    namespace_id, session_id, space_id, space_epoch, unreliable_channel_id,
    entity_id, sequence, type_id,
    RoutingPosition3D { x: 9.9, y: 0.0, z: 0.0 },
    state_bytes,
)?;
```

`publish_positioned_state` sends `EntityState/LatestValue` on the reliable control stream;
`publish_unreliable_positioned_state` sends `EntityState/UnreliableSequenced` as one datagram.
The finite 3D routing position is additive metadata; payload bytes remain opaque. The server
validates space frame/bounds and applies position plus publication atomically. A local successful
send is not a server acknowledgement.

Channel policy is still server-controlled. In the current managed composition, spatial spaces
expose channel 1 as `ReliableOrdered` events and channel 4 as `UnreliableSequenced` state, so use
the unreliable positioned method there. The reliable positioned method is for server
compositions that provision a matching `LatestValue` channel (for example development/static
channel 2); it does not override managed channel 1.

## Cancellation-safe control receive

`Client::recv` and `recv_timeout` retain an incomplete control-stream frame on the
client across timeout or cancellation. The next call resumes the same frame,
including when cancellation occurred partway through its four-byte prefix or body.
Short polling intervals therefore do not discard reliable chat/profile/control bytes.
The exclusive `&mut Client` reader and public API are unchanged; no background reader
or decoded inbox is added.

Only a partial four-byte prefix plus one frame are retained. The complete prefix is
validated against the configured incoming frame limit **before** allocating its body;
complete frames still use the configured incoming payload limit. Transport reads
commit their byte counts before another await, so datagram reception can run
independently without resetting stream framing. Negotiated outgoing limits are unchanged.
Managed admission still fails closed on cancellation/timeouts: writes or admission
outcomes can remain ambiguous even though receive framing is now resumable.

## Unreliable entity-state datagrams

`Client::publish_unreliable_state(&self, namespace_id, session_id, space_id,
space_epoch, channel_id, entity_id, sequence, type_id, payload: Vec<u8>)`
is synchronous and returns `Result<(), ClientError>`. It sends exactly one
size-prefixed WVN1 `EntityState` envelope with `UnreliableSequenced` delivery over
native QUIC or WebTransport. The route must already be provisioned by the server
with matching policy; this method cannot change channel delivery or persistence.
Existing `publish_state` and `publish_positioned_state` remain `LatestValue` over the control stream.

Publishing preserves the negotiated outgoing frame/payload limits and the 64 KiB
payload ceiling. The **whole encoded frame**, including its four-byte prefix and
metadata, must additionally fit the transport's current `max_datagram_size()`.
That budget can change with the path MTU; it is checked for every send, and a
send-time transport failure is also returned. Unsupported/disabled datagrams and
MTU-size failures return `ClientError::Transport`; size errors include the actual
frame length and current limit. Local rejection leaves the connection usable.
There is no fragmentation, truncation, stream fallback, retry, or application queue.

**`Ok(())` means local transport submission, not server acceptance or delivery.**
Congestion may discard this or older queued datagrams. Continue receiving control
traffic to observe server errors; track submissions separately from received updates.
Sender sequences must be strictly monotone per connection × space × epoch × entity
× channel. Receivers must reject stale sequences/epochs in bounded application state;
transport datagrams can arrive out of order, including across lifecycle messages.

After admission/setup, call
`client.take_datagram_receiver() -> Result<DatagramReceiver, ClientError>` once:

```rust,ignore
// Assume the server provisioned this route, and the client owns `entity_id`.
let mut poses = client.take_datagram_receiver()?;
client.publish_unreliable_state(
    namespace_id, session_id, space_id, space_epoch, channel_id, entity_id,
    sequence, type_id, transform.encode().to_vec(),
)?;
if let Some(envelope) = poses.recv_timeout(std::time::Duration::from_millis(5)).await? {
    // Apply only current-epoch, newer-sequence updates for known entities.
}
```

The public, non-`Clone` `DatagramReceiver` exposes
`recv(&mut self) -> Result<Envelope, ClientError>` and
`recv_timeout(&mut self, Duration) -> Result<Option<Envelope>, ClientError>` as
async methods. A receive consumes exactly one complete datagram and decodes one
complete frame under the configured **incoming** limits. Only valid-scoped
`EntityState / UnreliableSequenced` is accepted. Malformed, oversized,
trailing-byte, or wrong-kind/class packets return `ClientError::Protocol`;
the packet is consumed without closing the connection, so the next receive can
continue. Packet errors are never converted into timeout results.

Datagram receive timeout/cancellation is safe: no partial frame is consumed, no
pending application read is retained, and no control-stream read is cancelled.
There is no background reader, application inbox, or unbounded sequence cache;
only the underlying bounded transport buffers are used. The handle does not borrow
the client, so datagram reception can run alongside the exclusive control reader.
Control `recv`/`recv_timeout` remain stream-only and retain partial framing across
cancellation as described above. The datagram receiver remains independent.

Handout is permanent, even after dropping the receiver. All managed admission and
queue APIs are rejected before stream I/O after handout. The consuming
`admit_with_cancellation` runner also explicitly closes the shared connection when
rejecting handout, preserving cleanup despite the receiver's connection clone.
Finish managed admission first. The client still owns the endpoint and shutdown:
keep it and its runtime alive, and explicitly `close`/`close_gracefully` it when
stopping. The receiver alone is not an independent connection owner or close API;
already-buffered datagrams may drain after closure before a transport error.

## Bounded shutdown

`Client::close(self)` remains synchronous and only initiates connection closure.
Before tearing down a client's Tokio runtime, prefer:

```rust,ignore
client.close_gracefully(std::time::Duration::from_secs(2)).await?;
```

Both transports retain their endpoint. The async API initiates connection close,
then awaits Quinn's `Endpoint::wait_idle()` (via the WebTransport wrapper where
applicable), bounded by the supplied duration. Keep the connection's owning
runtime running and await with Tokio time enabled. No sleep or idle-timeout change
is used. `Ok(())` means the endpoint became idle; expiration returns
`ClientError::Transport("graceful close timed out")` and releases the client.
Cancelling the future forfeits its remaining shutdown opportunity.

This is a good-faith opportunity to send close frames, **not** an acknowledgement
of peer receipt, server cleanup, `EntityLeft`, or pending application data.
Connection close can discard buffered application data. Packet loss or an expired
close budget can still leave the peer waiting for its normal idle timeout.

## Verified remote QUIC (Weaver integration API)

Existing `ClientConfig` fields are unchanged. Use the additive verified-TLS entry point:

```rust,ignore
use woven_client::{Client, ClientConfig, ClientTlsConfig};

// Read operator-provided files, not token values from CLI arguments or URLs.
// Apply bounded file reads in a UI/runner; from_ca_pem itself caps input at 1 MiB.
let ca_pem = std::fs::read("/run/woven/client/ca.pem")?;
let token_file = std::fs::read_to_string("/run/woven/client/token")?;
let token = token_file.trim_end_matches(['\r', '\n']).to_owned();
let tls = ClientTlsConfig::from_ca_pem(&ca_pem)?;
let mut client = Client::connect_with_tls(
    ClientConfig {
        url: "quic://woven.example.test:8081".to_owned(), // illustrative DNS name
        token,
        ..ClientConfig::default()
    },
    tls,
).await?;
client.join_session(1, 1).await?;
client.subscribe_space(1, 1, 1, 1, 1).await?;
// Receive SubscriptionAccepted and EntityEntered to obtain the server-assigned entity.
// Existing publish_event/publish_state/recv/recv_timeout/close methods are unchanged.
```

Exact public additions:

```rust,ignore
// Opaque, Clone; no credential or TLS-material Debug output.
pub struct ClientTlsConfig { /* private root store */ }
impl ClientTlsConfig {
    pub fn from_ca_pem(pem: &[u8]) -> Result<Self, ClientError>;
    pub fn with_root_certificates(roots: rustls::RootCertStore) -> Result<Self, ClientError>;
}
impl Client {
    pub async fn connect_with_tls(
        config: ClientConfig,
        tls: ClientTlsConfig,
    ) -> Result<Self, ClientError>;
}
```

- `from_ca_pem` trusts **only** that PEM bundle; empty/invalid stores fail. Standard
  rustls chain, certificate validity, and hostname/IP SAN verification remain enabled,
  even on loopback. There is no insecure remote switch or failure fallback.
- `with_root_certificates` accepts an application's populated rustls 0.23 root store,
  including public roots if the consumer already supplies them. No OS/public-root loader
  or new root-bundle dependency is added to Woven.
- The URL host is both the DNS target and TLS verification name. There is no separate
  SNI override. DNS names, IPv4, and bracketed IPv6 URLs are supported. Resolution uses
  the **first** address; no multi-address retry/Happy Eyeballs is implemented.
- DNS, TLS and the entire WVN1 authentication handshake have a ten-second timeout.
  Callers can wrap the future in a shorter timeout or cancel by dropping it. Bounded
  scheduling, run duration, rate, worker counts, stop control, and receive deadlines
  remain the consuming runner's responsibility.
- The verified API supports **native QUIC only**. WebTransport with this TLS config is
  rejected. `Client::connect` retains development TLS for **literal loopback IPs only**;
  remote addresses and DNS names must use `connect_with_tls`. Existing default loopback
  QUIC/WebTransport workflows are unchanged. Never proxy insecure development TLS to a
  remote server. `localhost` DNS is intentionally not an insecure-TLS exception.
- Keep tokens out of URLs/configuration logs and recorded test scripts/results.
  `ClientConfig` Debug redacts the token and omits the URL; it still holds a cloneable
  plaintext token in memory, without zeroization. This does not make arbitrary
  application logging safe. Protect credential files with owner-only permissions.
- TLS/config failures use `ClientError::Transport`; rejected credentials use the existing
  `ClientError::ServerError`. Protocol authorization failures can close the connection;
  record the rejection rather than silently retrying.

The initial remote server is a [fixed scoped-credential composition](../woven-server/README.md),
not production tenant auth. No wire schema, `ClientConfig` fields, or publish API changed.
