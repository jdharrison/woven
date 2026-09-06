# WOVEN

WOVEN is a reusable distributed realtime session, event, state, and inference network. The implementation is Rust-first, transport-independent at its core, and designed for browser and native clients without embedding application-specific simulation rules.

## Standalone and self-hosted

WOVEN is a general-purpose realtime relay, not a product tied to any particular front end or control plane. Run it standalone and self-hosted, the way you'd self-host Redis or Postgres, with zero dependency on any hosted service. A managed instance is available at [woven.host](https://woven.host) for teams that don't want to operate their own, but that hosted offering, and any control-panel UI built on top of it, is a separate, optional consumer of this open-source core, not a requirement for using it. This repository is open-source under the [Apache License 2.0](LICENSE) and contains the complete server source.

## Current feature set

- A transport-neutral Rust core with typed IDs, authenticated namespace/session/space/channel grants, nested spaces, subscriptions, entity ownership, channel authority, sequencing, snapshots, bounded state, rate limits, and priority-aware bounded/coalescing outbound queues.
- Explicit entity lifecycle support: server-assigned entities, disconnect cleanup, subscriber `EntityLeft` notifications, and atomic epoch-validated transitions that emit ordered leave/enter events.
- A versioned, size-prefixed FlatBuffers protocol with verifier-backed bounded decoding, semantic validation, typed control payloads, and checked-in golden fixtures proving byte-for-byte cross-language stability.
- An Axum HTTP control plane with public `/healthz`, `/readyz`, `/metrics`, and `/v1/capabilities` endpoints.
- Two interchangeable realtime transports sharing one bounded single-owner Tokio worker and protocol bridge: native QUIC and browser WebTransport, which map unreliable/best-effort traffic to datagrams. Both have real-socket conformance coverage.
- Uniform 2D/3D spatial routing for replaceable state, with owner-updated local positions, cell indexes, radius filtering, optional exact distance checks, and reliable-event bypass.
- A bounded local load runner for broadcast, topic, 2D-grid, and 3D-grid scenarios with measured publish latency, delivery, queue, and machine metadata.
- An integrated inference plane: a bounded per-request provider queue, a provider-neutral capability/request model, and a deterministic tool-call gateway that lets model output propose state changes without ever mutating state directly.
- Native Rust and browser TypeScript clients. Both use the generated FlatBuffers bindings and validate cross-language decoding against checked-in golden fixtures.

See [`docs/status.md`](docs/status.md) and [`docs/adr`](docs/adr) for what's implemented and the architecture decisions behind it.

## Prerequisites

- The pinned current-stable Rust 1.98.0 toolchain with rustfmt and Clippy. [`rust-toolchain.toml`](rust-toolchain.toml) installs these automatically through rustup.
- A C++ compiler and CMake for the pinned vendored FlatBuffers compiler used during protocol builds.
- Node.js 22+, only if you're working on the TypeScript browser client.

A system `flatc` installation is not required. Cargo builds the pinned FlatBuffers 25.12.19 compiler from the `flatc-fork` crate and generates Rust bindings into `OUT_DIR`.

## Installation

```sh
cargo install woven-server
cargo add woven-client
npm install @signalweave/woven-client
```

`woven-server` is the self-hosted server executable. `woven-client` is the native Rust library, and `@signalweave/woven-client` is the browser/WebTransport client.

## Rust 0.2 migration

The core, transport, inference, and server crates move together to `0.2.0` because
`PersistenceClass::Stateful` is now `PersistenceClass::Stateful { ttl: None }`
(or `ttl: Some(duration)` for expiry). Update constructors and pattern matches,
and upgrade all dependencies that exchange core or inference types together;
`0.1` and `0.2` types are not interchangeable. The minor-version boundary prevents
existing `^0.1` consumers from resolving the breaking core API automatically.
The wire protocol and native/browser client versions are unchanged.
`woven-loadtest` remains workspace-only (`publish = false`).

For releases, verify and publish registry dependencies before their consumers:
core; transport and inference-core; transport-quic, inference-tools, and
inference-test-provider; inference-coordinator; server. Run `cargo package --locked
-p <crate>` before tagging each artifact, once its dependencies are available on
crates.io. Workspace tests alone do not verify registry dependency compatibility.
Each artifact uses its own `release/<crate>/v<version>` tag; several tags may
legitimately refer to the same commit.

## Local development

```sh
sh scripts/local/doctor.sh
sh scripts/local/run-dev.sh
sh scripts/local/loadtest.sh --scenario grid2d --participants 500 --rounds 100

cargo test --workspace --all-targets --all-features

cd crates/woven-client-ts
npm ci
npm run format:check
npm run lint
npm run typecheck
npm run test
npm run build
```

`run-dev.sh` starts the development-only composition on `127.0.0.1:8080` (HTTP control plane),
`127.0.0.1:8081` (QUIC), and `127.0.0.1:8082` (WebTransport). It uses the static `dev-token` and
an ephemeral self-signed certificate for the UDP-based transports. It is deliberately loopback-only
and must not be exposed to a network or used with real credentials.

`loadtest.sh` runs a release-mode, direct-core routing benchmark. It exercises routing and bounded
outbound queues, but does not measure QUIC, WebTransport, TLS, sockets, or the shared transport-worker
command queue. End-to-end transport stress testing is a separate harness and operational milestone.

Development activity logging is off by default so local measurements stay representative. Enable safe,
metadata-only CLI diagnostics explicitly when investigating a problem:

```sh
sh scripts/local/run-dev.sh --log-transform
sh scripts/local/run-dev.sh --log-all
```

`--log-transform` reports entity position and entity-scoped latest-state publications. `--log-all` reports
all development activity. Both options work only in debug builds; release binaries omit activity logging.

Common commands:

```sh
cargo fmt --all -- --check
cargo check-all
cargo lint
cargo test-all
cargo doc --workspace --no-deps
cargo run -p woven-protocol --example write_golden
cargo run -p woven-protocol --example write_tool_call_completed_fixture
```

The two `write_*_fixture` commands regenerate the checked-in protocol golden fixtures and should only produce a diff when the protocol intentionally changes.

## Workspace

- [`crates/woven-core`](crates/woven-core): transport-neutral sessions, spaces, ownership, authority, state, queues, and worker harness.
- [`crates/woven-protocol`](crates/woven-protocol): FlatBuffers schema, generated Rust bindings, bounded framing, validation, and fixtures.
- [`crates/woven-transport`](crates/woven-transport): shared worker handle, entity-lifecycle fan-out, and protocol bridge used by every transport adapter.
- [`crates/woven-transport-quic`](crates/woven-transport-quic): QUIC (Quinn) native and browser WebTransport adapter with reliable streams and unreliable datagrams.
- [`crates/woven-server`](crates/woven-server): Axum control plane and development server composition.
- [`crates/woven-inference-core`](crates/woven-inference-core): capability/request/provider data model and the `Provider` trait for the optional inference plane.
- [`crates/woven-inference-tools`](crates/woven-inference-tools): bounded tool registry and deterministic tool-call gateway; models propose, the gateway decides.
- [`crates/woven-inference-test-provider`](crates/woven-inference-test-provider): deterministic, scripted provider used in tests and local development.
- [`crates/woven-inference-coordinator`](crates/woven-inference-coordinator): runs an AI identity as an ordinary core connection and drives providers/tools.
- [`crates/woven-client-rust`](crates/woven-client-rust): `woven-client`, the native QUIC/WebTransport client library and integration-test driver.
- [`crates/woven-client-ts`](crates/woven-client-ts): `@signalweave/woven-client`, the browser/WebTransport package and generated TypeScript FlatBuffers bindings.
- [`crates/woven-loadtest`](crates/woven-loadtest): bounded local routing scenarios and measurement output.
- [`docs/adr`](docs/adr): accepted architecture records.

## CI and releases

Pull requests and pushes to `main` run Rust formatting, Clippy, tests, and workspace builds, plus TypeScript formatting, static checks, tests, and package builds. CI never publishes packages, creates releases, or requires registry credentials.

Pushing a `release/<artifact>/vX.Y.Z` tag starts an artifact release automatically. Each package owns its version; the workflow verifies the selected package matches the tag, validates the full workspace, and refuses an already-published registry version. It publishes only that artifact. `woven-server` releases additionally create a GitHub Release with server archives and SHA-256 checksums. Manual dispatch remains available for an existing artifact tag and requires `confirm=publish`.

Required GitHub Actions secrets:

- `CARGO_REGISTRY_TOKEN` — crates.io token authorized to publish the Woven crates.
- `NPM_TOKEN` — npm automation token when npm trusted publishing is not configured. Trusted publishing uses the workflow OIDC identity and provenance instead.

Supported `woven-server` binary platform:

- Linux x86_64 (`x86_64-unknown-linux-musl`)

Other platforms can build `woven-server` from source with Cargo; prebuilt binaries will be added when they are a supported operational target.

### Maintainer release checklist

1. Bump only the package being released.
2. Confirm changelog/release notes.
3. Push `release/<artifact>/vX.Y.Z` (for example, `release/woven-client/v0.1.3`).
4. Monitor the release workflow; only `woven-server` releases create binary assets and a GitHub Release.
5. Verify the selected registry package, and for server releases, checksums and downloaded binaries.

Cloud deployment and engine distributions are intentionally not part of this pipeline yet.

## Development configuration

The development server composition explicitly provisions namespace/session `1`, logical spaces `1` and `2`, reliable and latest-value channels, and the development token `dev-token`. This composition exists solely for local development and test automation; it is not a production authentication or TLS configuration. When the inference plane is enabled, it also provisions one demo AI identity with its own dev token, entity, and status channel; see [`crates/woven-inference-coordinator`](crates/woven-inference-coordinator).

[`.env.example`](.env.example) documents the non-secret `WOVEN_*` configuration contract for deployment-oriented runtime configuration, including `WOVEN_INFERENCE_ENABLED` (the inference plane is off by default). Development authentication must be selected explicitly; authentication is never silently disabled. TLS termination and production certificate configuration are deferred with deployment infrastructure.

## Security baseline

The core assigns connection and entity identities server-side, requires authentication before participation, enforces explicit namespace/session/space/channel grants and entity ownership, validates sequence and epoch freshness, rate-limits publication, bounds state and payload sizes, and bounds every implemented queue. Protocol buffers received from untrusted peers are verified before field access. The inference plane follows the same rules: an AI identity is just another authenticated connection, and model-proposed state changes only take effect after a deterministic tool gateway validates them, never directly.

## License

[Apache License 2.0](LICENSE)
