# Woven Protocol

This crate defines the transport-neutral Woven Protocol v1 envelope, typed control messages, bounded size-prefixed framing, safe owned Rust representations, and conformance fixtures. A transport adapter supplies and consumes complete frames; this crate does not depend on WebSocket, QUIC, WebTransport, or `woven-core` internals.

## Managed admission wire slice

Additive WVN1 controls (Rust variant and struct names match):

| ControlPayload | MessageKind | Union tag | Rust fields |
|---|---:|---:|---|
| `RequestAdmission` | 33 | 30 | `idempotency_key: String` |
| `AdmissionResult` | 34 | 31 | `status: AdmissionStatus`, `rejection_code: AdmissionRejectionCode`, `ticket_id: Option<u64>`, `poll_after_ms: u32`, `ticket_remaining_ms: u32` |
| `QueueStatusRequest` | 35 | 32 | `ticket_id: u64` |
| `QueueHeartbeat` | 36 | 33 | `ticket_id: u64` |
| `QueueClaim` | 37 | 34 | `ticket_id: u64` |
| `QueueCancel` | 38 | 35 | `ticket_id: u64` |
| `QueueUpdate` | 39 | 36 | `ticket_id: u64`, `state: QueueState`, `position: u32`, `poll_after_ms: u32`, `ticket_remaining_ms: u32`, `offer_remaining_ms: u32` |

Enums (u8):
- `AdmissionStatus`: Unknown=0, Admitted=1, Queued=2, Paused=3, Rejected=4.
- `AdmissionRejectionCode`: None=0, ServerPaused=1, QueueFull=2, QueueDisabled=3,
  AlreadyQueued=4, InvalidIdempotencyKey=5.
- `QueueState`: Unknown=0, Waiting=1, Offered=2, Admitted=3, Cancelled=4, Expired=5, Missing=6.

All seven controls require ReliableOrdered, nonzero namespace/session and correlation
in the envelope, and no space/channel/entity/epoch scope or domain payload. Replies
must echo request scope/correlation; the bridge must bind them to authenticated
connection scope. Ticket IDs are nonzero u64 (TS bigint). Keys are 1–256 UTF-8 bytes.
No lease, principal, resume token, or client-controlled lifetime is present.

Only Queued admission results carry a ticket; only Rejected carries a non-None
rejection. Waiting positions are one-based; all other positions are zero. Poll advice
is 0–30,000 ms and is nonzero only for Queued/Paused admissions or Waiting/Offered
updates. Ticket lifetime is at most 900,000 ms, offer lifetime at most 30,000 ms;
zero means **unavailable**, not an invented fresh TTL. Lifetimes are absent (zero)
on terminal results; offer lifetime is only valid for Offered. Transport should
emit actual remaining lifetimes only when the worker provides them. Recommended
poll advice is 1,000 ms. Unknown result states and inconsistent fields are rejected.

The implemented `woven-transport` bridge routes RequestAdmission to the
connection-bound worker admission API and sends Admitted only after atomic lease
binding/join; no second JoinSessionWithAdmission command is needed. Queue operations
invoke SessionQueue; Claim also joins atomically in core. The bridge sanitizes
JoinDecision/QueueStatus into wire results without serializing core lease/ticket
structs. Foreign/nonexistent tickets both map to Missing. Authentication/scope/rate
failures use correlated ProtocolError replies and close the connection, rather than
fabricating admission outcomes. Ordinary JoinSession remains for unmanaged sessions
and cannot bypass managed admission.

Regenerate the additive golden with `cargo run -p woven-protocol --example
write_managed_fixture`. The protocol crate remains transport-neutral; the managed
node composition, bridge, and native client are implemented in their respective
crates and covered by `woven-server/tests/managed_quic.rs`. The real Host-to-managed-
node E2E in `woven-server/tests/host_managed_local.rs` has also passed via
`npm run test:local` from the sibling `../woven-host` checkout (relative to
Woven's root). It exercises Host HTTP APIs and TLS-verified native QUIC with isolated
Firebase Auth/Firestore emulators on loopback, not cloud or browser validation.
This cross-repository test is ignored by ordinary Cargo runs.

## Wire format

The canonical schema is `schemas/woven_v1.fbs`. It uses the `WVN1` FlatBuffers file identifier and a four-byte little-endian FlatBuffers size prefix. The prefix is the byte count after the prefix; `CodecLimits::max_frame_len` counts the complete frame, including those four bytes.

`Envelope` contains protocol version, stable message kind and delivery class values, namespace/session/space/channel IDs, optional entity semantics, space epoch, server tick, sender sequence, correlation/causal ID, payload type ID, payload bytes, and a typed control union. `EntityState`, `ReliableEvent`, and `Snapshot` use a non-zero payload type ID plus opaque domain bytes. Routing can inspect the envelope without understanding those domain bytes. Every other v1 message has a typed control table.

Scalar ID value `0` means absent or unassigned. Assigned IDs and established epochs start at `1`. The owned Rust API represents optional entity, correlation, and channel values with `Option<u64>` where appropriate.

## Safe codec and limits

`Codec::decode` applies bounds before accessing a FlatBuffer, checks the exact size prefix and file identifier, and then calls the generated FlatBuffers verifier API. Only after successful verification does it copy values into owned Rust types. It rejects:

- incomplete frames and trailing bytes;
- frames, opaque payloads, or control strings/vectors above configured limits;
- malformed FlatBuffers and incorrect file identifiers;
- protocol versions other than v1;
- unknown message or delivery values;
- message-kind/control-union mismatches; and
- domain payloads on controls or missing domain payload type IDs; and
- invalid per-message scope, ID, enum, version-range, or delivery semantics.

`Codec::expected_frame_len` lets stream transports read exactly one bounded frame after receiving the four-byte prefix. This crate does not allocate queues; transport implementations remain responsible for bounded queue and backpressure policy.

FlatBuffers' generated Rust accessors necessarily contain the runtime's low-level `unsafe` implementations. Generated files remain private in `OUT_DIR` and receive a narrowly scoped lint allowance. All checked-in Rust and all public framing/codec logic are safe Rust, generated unchecked root functions are not exposed, and untrusted buffers always use verifier-backed access.

## Reproducible generation

Normal Cargo builds do not require a system `flatc` and do not download a compiler at build-script runtime. The crate pins:

- `flatbuffers = =25.12.19`
- `flatbuffers-build = =0.2.4+flatc-25.12.19`
- `flatc-fork = =0.6.0+25.12.19-2026-02-06-03fffb2`

`build.rs` passes the compiler returned by `flatc_fork::flatc()` to `flatbuffers_build::BuilderOptions::set_compiler`. Rust output is generated below `OUT_DIR/flatbuffers`; it is not checked into source control.

```sh
cargo build
```

For manual cross-language review, first run `cargo build`. The vendored executable can then be located under the crate-local target directory (the hash is Cargo-generated):

```sh
VENDORED_FLATC=$(find target/debug/build -path '*/out/bin/flatc' -type f -print -quit)
"$VENDORED_FLATC" --version
"$VENDORED_FLATC" --rust -o /tmp/woven-rust schemas/woven_v1.fbs
"$VENDORED_FLATC" --ts -o /tmp/woven-ts schemas/woven_v1.fbs
"$VENDORED_FLATC" --csharp -o /tmp/woven-csharp schemas/woven_v1.fbs
"$VENDORED_FLATC" --python -o /tmp/woven-python schemas/woven_v1.fbs
```

A separately installed compiler may be substituted only when `flatc --version` reports `25.12.19`. TypeScript generation, bindings, and cross-language golden-fixture decode tests are implemented in `crates/woven-client-ts`. C# generation, bindings, and golden-fixture decode tests are implemented in `crates/woven-client-csharp`, which vendors a matching FlatBuffers C# runtime rather than depending on NuGet's `Google.FlatBuffers` package (its published releases lag behind the pinned `25.12.19` compiler). Python generation, bindings, and golden-fixture decode tests are implemented in `crates/woven-client-python`, which depends directly on PyPI's `flatbuffers==25.12.19` package — no version mismatch to work around there, unlike C#.

## Compatibility rules

Schema changes are reviewed against these rules:

1. Add fields only at the end of an existing table. Never reorder fields or change an existing field's type or meaning.
2. Never renumber enum values, union variants, message kinds, or existing table fields. Numeric values are the wire contract.
3. Reserve and deprecate removed values; never reuse them for a new meaning. Prefer leaving deprecated fields in place.
4. Every added scalar field must have a backward-compatible default. Added table, string, and vector fields must tolerate absence. Do not change an existing default.
5. Keep `0` reserved for unknown/none/absent semantics. New assigned IDs begin at `1`.
6. Domain payload type IDs are stable identifiers for separately versioned application schemas. The routing core treats their bytes as opaque.
7. `Hello` advertises a supported version range and `Capabilities` selects a mutually supported version. Once selected, every envelope uses that exact version. Peers reject an unsupported version rather than silently reinterpreting it.
8. Breaking syntax or semantics require a new negotiated protocol version, a new versioned namespace/schema, and new golden fixtures.

## Golden fixtures

- `tests/fixtures/reliable_event_v1.swp` (+ `.expected.txt`) — exercises every envelope metadata field and an opaque reliable-event payload.
- `tests/fixtures/tool_call_completed_v1.swp` (+ `.expected.txt`) — exercises a typed inference/tool-call control message, independent of the fixture above.

Regenerate them deterministically with:

```sh
cargo run -p woven-protocol --example write_golden
cargo run -p woven-protocol --example write_tool_call_completed_fixture
cargo test -p woven-protocol --test golden
```

The tests require both byte-for-byte encoding stability and equivalent verified decoding. `crates/woven-client-ts` decodes both fixtures from TypeScript to prove cross-language equivalence.
