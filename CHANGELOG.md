# Changelog

All notable release changes are documented here. Woven packages version independently;
release tags use `release/<artifact>/v<version>` and must follow the dependency order in
the repository [README](README.md#current-release-versions-and-publication-order).

## Unreleased

### Release hardening

- Include the Apache-2.0 license text in every publishable Rust crate and in the npm package.
- Gate CI and releases on rustdoc warnings and a bounded real-Chromium WebTransport E2E.
- Fix vendored `flatc` discovery for cross-compilation and require the supported musl server
  binary to build before publishing `woven-server`, while keeping GitHub release asset upload
  after registry publication.
- Document mandatory npm token authentication, package dry runs, and the complete sequential
  publication graph.

## Current release candidates

### Runtime crates `0.3.0`

Applies to `woven-core`, `woven-transport`, `woven-transport-quic`,
`woven-inference-core`, `woven-inference-tools`, `woven-inference-test-provider`,
`woven-inference-coordinator`, and `woven-server`.

- Add managed QUIC and optional managed WebTransport runtime composition with explicit TLS,
  exact browser-origin allowlisting, authenticated loopback administration, and bounded
  lifecycle management.
- Add scoped session provisioning, capacity allocation, admission queues, queue claim/cancel/
  heartbeat operations, usage accounting, revocation, and teardown behavior.
- Add real TLS-verified native QUIC and WebTransport coverage for authentication, admission,
  queueing, scope isolation, publication, deletion, and shutdown.
- Keep development, static remote, and managed modes explicit and fail closed; hosted identity,
  durable recovery, federation, and cloud deployment remain outside this release.

`woven-loadtest` is versioned `0.3.0` with the workspace but is not publishable.

### Protocol and clients `0.2.0`

Applies to `woven-protocol`, the native Rust `woven-client`, and the npm package
`@signalweave/woven-client`.

- Add WVN1 managed admission and queue controls with strict scope, correlation, bounds, and
  semantic validation.
- Add bounded native Rust and TypeScript admission helpers with cancellation and no implicit
  transport retries.
- Add a real headless-Chromium TypeScript/WebTransport smoke test against a disposable managed
  Woven listener, covering Bearer authentication, admission, subscription, publish/echo,
  graceful disconnect, and CCU release.
- C# and Python client packages are not included in this release line.
