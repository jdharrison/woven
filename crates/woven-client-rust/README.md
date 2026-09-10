# woven-client

Native Rust client for Woven using QUIC and WebTransport.

```sh
cargo add woven-client
```

`woven-client` is a library crate. It does not provide a standalone executable.

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
