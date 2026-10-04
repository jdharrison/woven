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

These versions identify the next candidates; this section is not a record of registry
publication, deployment, or production validation.

### Runtime crates `0.4.0`

Applies to `woven-core`, `woven-transport`, `woven-transport-quic`, all four
`woven-inference-*` crates, and `woven-server`. `woven-loadtest` also becomes `0.4.0`
but remains workspace-only (`publish = false`). Inference crates follow the new core/transport
dependency types; this version change does not add domain logic or new inference behavior.

- Add bounded managed Cartesian3D spatial subspaces with inclusive position bounds, exact
  grants, idempotent add-only provisioning, and advertised definitions/capabilities/limits.
  Managed channels are exactly 1 (`ReliableOrdered` events) and 4
  (`UnreliableSequenced` ephemeral state); development/static compositions also provision
  spatial space 3, and development retains its separate stateful channel.
- Apply routing position and replaceable state atomically before routing. Reject attached
  positions on non-state delivery before sequence, journal, or outbound effects, preventing
  envelopes that the WVN1 writer would reject.
- Add optional managed `tickRateHz` as an additional per-member/session publish-admission
  budget, not a simulation scheduler. Preserve rate history through configuration changes,
  unlimited intervals, and leave/rejoin; retain detached histories within explicit caps.
- Roll back failed admission/queue claims by invalidating the failed ticket and releasing
  the exact admission lease rather than leaking or releasing another membership's capacity.
- Keep unreliable state on datagrams with no reliable fallback. Preserve reliable-frame
  read progress across interleaved datagrams, and hold bounded pending-subscription updates
  until subscription acceptance and the subscriber's own entity entry are queued.
- Add session-scoped client logging with trusted node metadata, successful membership and
  disconnect lifecycle events, bounded rates, and a volatile event ring. Expose an authenticated,
  incarnation-bound, JSON-size-bounded managed log feed; this is best-effort collection, not
  lossless archival storage, and ordinary state updates are not logged.

### Protocol and clients `0.3.0`

Applies to `woven-protocol`, the native Rust `woven-client`, and the npm package
`@signalweave/woven-client`.

- Append finite 3D routing metadata for `EntityState` and session-only `ClientLog` controls
  with info/warn/error levels and a 1,024-byte UTF-8 message ceiling. Capability masks `1` and `2`
  gate positioned state and logging, respectively; the wire version remains 1 (`WVN1`).
- Add independent, pull-driven unreliable entity-state datagram APIs in both clients,
  including positioned state, complete-frame/MTU checks, bounded browser transport queues,
  and explicit loss/error behavior without fragmentation, retries, or reliable fallback.
- Add message-only session loggers and bounded join/admission/leave scope tracking. Send
  completion is not a persistence acknowledgement; malformed wire frames retain the existing
  fatal decode behavior, while SDK validation rejects oversized messages before sending.
- Preserve native reliable-stream read progress across cancellation and keep incoming and
  negotiated outgoing codec limits explicit. Enforce browser payload ceilings before
  serialization and configured/server complete-frame limits before every reliable write,
  including positioned state, controls, and authentication.
- Fix TypeScript datagram interoperability by allowing an initial zero sender sequence,
  matching the Rust codec/core. IDs remain nonzero, sequences remain bounded u64 values,
  and subsequent updates must increase within the same server-side sequence scope.
- Extend Rust/TypeScript interoperability coverage with ClientLog and zero-sequence state
  frames, preserving historical golden fixtures. Real Chromium loopback coverage now proves
  an actual datagram echo separately from reliable event delivery.

### Compatibility and release sequencing

Rust packages now require Rust 1.98, matching the verified pinned 1.98.0 toolchain.
The previously declared 1.88 minimum does not compile the current usage-spool async
implementation; this release does not claim compatibility with that compiler.

Added Rust fields and enum variants, plus removed `Eq` derives, are source-compatibility changes;
rebuild consuming clients and synchronize internal dependency requirements. Host consumers of
managed channels, spatial definitions, capabilities, and limits must use the matching strict
control-plane schemas. No wire-version bump or managed persistence/restart guarantee is implied.
Follow the dependency-first artifact order in the README and verify prerequisite registry
availability before packaging dependents. Local workspace gates do not prove published-package
compatibility or deployed endpoint behavior.

## Earlier candidate baseline

The following entries describe the preceding candidate contents, not confirmation that those
versions were published.

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
