# Managed configuration durability and offline import

This is a local operator interface, not approval to deploy, transfer credentials,
change VM configuration, or run a production migration. No runtime bootstrap API
is exposed. Review and approve any migration separately in Host/operations.

## Runtime and identity contract

- `ManagedServerConfig.state_directory: Option<PathBuf>` / optional
  `WOVEN_MANAGED_STATE_DIR` enables durable managed configuration. The value must
  be an absolute path to an **existing Linux local-filesystem directory**, owned
  by the effective service user with exactly `0700` permissions. Every path
  component must be free of symlinks. `/proc/self/fd` must be available.
- Omission retains the existing ephemeral behavior. Configuring a state directory
  without complete managed configuration fails closed; it never selects development
  mode. In durable mode, missing, corrupt, incompatible, unsafe, or locked state is
  a startup error **before any listener is bound**. There is no empty fallback.
- `ManagedServer.node_incarnation` and authenticated `GET /v1/node.nodeIncarnation`
  are the persisted 48-character lowercase-hex **managed lineage**, restored only
  with its verified complete checkpoint. The existing `Woven-Node-Incarnation`
  scoped-request guard remains required and uses this stable lineage.
- `ManagedServer.boot_id` and authenticated `GET /v1/node.bootId` are separately
  generated 48-character lowercase-hex random identifiers on **every server
  start**, including ephemeral mode. `bootId` is never configured, persisted, or
  accepted in an import. Process counters, uptime, volatile logs and log cursors
  reset on restart: Host must use `bootId`, not stable `nodeIncarnation`, to detect
  those resets. An offline importer is not a serving node and has no boot receipt.
- Boot changes do not invalidate a correctly retained managed revision/lineage.
  Conversely, copying only an old lineage into an empty server is not restoration.
  Host must approve both the lineage and full checkpoint as one migration unit.
- No wire protocol, client bindings, channel policy, or admin authentication
  changes are introduced. `/v1/node` adds `bootId` and a safe `managedState` status;
  no client credential verifier is exposed.

### Boot-scoped observability contract for Host

Managed loopback `GET /metrics` retains Prometheus text exposition unchanged and
returns exactly one of each response header (HTTP names are case-insensitive):

```text
Woven-Node-Incarnation: <stable managed lineage>
Woven-Boot-Id: <fresh serving boot>
```

Both identifiers are 48 lowercase hex characters. Counters are scoped to the
boot, not just the durable lineage. If the worker cannot supply live counts,
`/metrics` returns HTTP `503` with no metrics sample rather than fabricated zeros;
Host must discard the failed scrape, not treat it as a counter reset.
Development/static composition does not gain
these managed identity headers. Management HTTP remains private loopback; these
headers do not add authentication or authorize public exposure.

Bearer-authenticated `GET /v1/logs?after=0&limit=32` still **requires** exactly one
matching `Woven-Node-Incarnation` request header. It additionally accepts an
**optional** `Woven-Boot-Id` request header:

- Omitted: compatible with old clients; every successful page still carries
  `bootId` so consumers can detect restarts.
- Present: must be exactly one value equal to the running boot, case-sensitive.
  A stale, empty, malformed, comma-combined, or duplicate value returns HTTP
  `409` with `{"error":{"code":"boot_conflict"}}` and `Cache-Control: no-store`.
- The existing bearer-authentication/rate/concurrency limits still run first;
  an invalid credential does not gain an identity-conflict oracle. The mandatory
  lineage check remains independent (`incarnation_conflict` when it fails).

The exact successful page shape is the existing page plus top-level `bootId`
(placeholders below describe the identifiers, not literal values):

```json
{
  "nodeIncarnation": "<stable managed lineage>",
  "bootId": "<fresh serving boot>",
  "entries": [],
  "nextSequence": "0",
  "droppedThrough": "0"
}
```

Entry fields and decimal-string sequence cursors are unchanged. Capture begins
at sequence `1` in every new boot; an initially empty page has cursor `"0"`.
The 48 KiB response ceiling includes both identities and JSON escaping. In one
boot, an empty page never rewinds its requested cursor—even if `after` exceeds
the latest sequence. This rule also applies to legacy requests without a boot
header: the server does not guess that a high cursor belongs to an old process.

Host should scope counter baselines and log cursors by `(nodeIncarnation, bootId)`.
Send the expected boot with log polls and verify the returned page identity.
On `boot_conflict`, rediscover `/v1/node`; after verifying the expected durable
lineage and a **different boot**, start the new cursor at `0` and establish new
counter baselines. Never rewind on an empty page, a low counter alone, or while
the boot is unchanged. Metrics samples carry their own boot to avoid a race with
a separate `/v1/node` discovery; do not relabel an old-boot sample as the new boot.

## What is persisted

The full v1 `woven_core::ManagedCheckpoint`: optional immutable migration-baseline
provenance, active scopes, allocated CCU,
optional tick rate, revision and exact last-mutation identity, client-token SHA-256
verifiers, retired scope/verifier tombstones, and exact additional spatial-space
configuration. Compatibility system spaces are reconstructed by core restore.
Live connections, admissions, entities, queued messages, payload/world state,
rate windows, logs and metrics are **not** persisted. This feature is managed
configuration durability, not a payload journal or world save.

Accepted mutations (including idempotent accepted retries) are acknowledged by
the worker only after successful durable replacement. Save errors or the
transport's 30-second save timeout permanently close that worker. The server
closes QUIC/WebTransport endpoints and aborts admin/management listener tasks;
`/readyz` also independently rejects a closed worker. A failed/timeout mutation
may have reached disk even without an ACK; after restart, inspect the restored
revision and use the existing exact idempotency rules, not a blind new revision.
Blocking writes retain their writer lock until they finish even if the async
save future is cancelled or times out.

## Offline exact init/import interface

The exact-restoration CLI (baseline import is separate below) is:

```text
woven-server --managed-state-import
```

Supply the manifest through **stdin**, using an approved private pipe/stream.
The flag must appear alone; interactive terminal input is rejected. Use the
normal complete managed environment (including `WOVEN_MANAGED_QUIC=1`, configured
listener addresses, TLS paths, current private `WOVEN_ADMIN_TOKEN_FILE`) and
`WOVEN_MANAGED_STATE_DIR`. Import reads/validates the current admin credential
locally to enforce separation from client verifiers; it does not send it anywhere
or open a network listener, and does not load or serve the configured TLS key.

The library equivalent is:

```rust,ignore
pub async fn import_managed_state(
    config: ManagedServerConfig,
    manifest: Vec<u8>,
) -> Result<ManagedStateImportResult, ServerError>;
// ManagedStateImportResult { node_incarnation: String, managed_state: Option<Value> }
// Exact import returns managed_state: None and omits that JSON field.
// MAX_MANAGED_STATE_BYTES = 128 * 1024 * 1024
```

The exact strict JSON manifest envelope is:

```json
{
  "version": 1,
  "nodeIncarnation": "<approved existing 48-character lowercase-hex lineage>",
  "checkpoint": {
    "version": 1,
    "scopes": []
  }
}
```

The placeholder above is descriptive, not a valid import. `scopes: []` is allowed
**only as an explicitly approved empty initialization**, never as a recovery
shortcut for an existing deployment. The operator chooses a new random lineage
for a genuinely new deployment; the importer never generates one implicitly.

For a populated checkpoint each scope has all these fields:

| JSON field | Type / meaning |
|---|---|
| `namespaceId`, `sessionId` | nonzero JSON `u64` numbers |
| `revision` | nonzero JSON `u64` number; preserve the actual revision |
| `lastMutation` | exact original last request identity; variants below |
| `allocatedCcu` | JSON `u32` number; **not** admin HTTP's `allocatedCCU` |
| `tickRateHz` | `null` or integer 1..120 |
| `verifierSha256` | 64 lowercase hex characters: SHA-256 of the client token |
| `retired` | boolean; include all retained tombstones |
| `spatialSpaces` | array of all additional spatial definitions |

`lastMutation` is one of:

```json
{"kind":"provision"}
{"kind":"configuration","allocatedCcu":4,"tickRateHz":null}
{"kind":"space","spaceId":3}
```

The configuration variant preserves the last PATCH's optional tick-rate input,
not just the resolved tick rate. A deleted scope retains its prior mutation
identity and revision as exported by core; do not invent a deletion variant or
increment that revision. Each `spatialSpaces` entry has:

- `spaceId`: numeric `u64`, `epoch`: numeric `u64` (managed epoch is 1);
- `metersPerUnitBits`, `cellSizeBits`, `interestRadiusBits`: JSON unsigned `u64`
  **IEEE-754 f64 bit patterns**, not decimal floating-point values;
- `exactDistance`: boolean;
- `boundsBits`: six unsigned `u64` bit patterns in order
  `[min_x, min_y, min_z, max_x, max_y, max_z]`.

Exact import rejects any non-null `checkpoint.migrationBaseline`; it cannot be
used as the baseline entry point. Without metadata, v1 checkpoint JSON and its
checksum encoding are unchanged. Use the public core DTO/serde contract as the
source of truth. Unknown fields,
unsupported versions, duplicate scopes/verifiers, admin-verifier reuse, invalid
revision/mutation combinations, invalid spatial definitions, and core capacity
violations are rejected before committing. The encoded manifest and encoded
on-disk envelope are each capped at **128 MiB (134,217,728 bytes)**. Core collection
ceilings remain unchanged (4,096 scope-history entries including retired scopes,
1,024 active scopes, 64 additional spaces per scope). The encoded limit covers
every bounded compact v1 checkpoint **including** lineage and checksum-envelope
overhead, not just the active-session subset. A counting-writer regression serializes
the actual DTO at all 4,096 × 64 collection slots, using maximum integer/float-bit
widths, the largest mutation variant, maximum fixed hex-string lengths, and the
longer boolean encoding as a conservative structural upper bound. It counts the
manifest and complete envelope without allocating their encoded byte buffers.
The current conservative structural counts, including maximum baseline metadata
and escaped migration IDs, are **91,652,699 bytes** for the stored payload,
**91,652,773 bytes** for the approved baseline stdin manifest, and **91,652,808
bytes** for the complete checksum envelope, all below 134,217,728 bytes. Semantic validation is stricter than this structural fixture;
for example, version/CCU/tick-rate fields use their full integer widths in the
fixture even though valid values are smaller.

Raw import bytes (including whitespace) still have the explicit 128 MiB ceiling;
excessively padded JSON can exceed that input limit even when its compact form
would fit. Compact valid checkpoints fit regardless of retained history. This is
an encoded-byte limit only: no core capacity, spatial, permission, schema or
fail-stop checks are relaxed.

**Host serialization warning:** JavaScript `Number` cannot represent arbitrary
`u64` values, particularly float-bit patterns. Build/serialize the privileged
manifest with lossless unsigned-64-bit JSON support. Do not stringify BigInts as
JSON strings: the DTO expects numbers. Do not derive this format from the public
session snapshot, which intentionally lacks verifiers and exact mutation history.

Successful CLI stdout is exactly one safe JSON receipt plus a newline:

```json
{"nodeIncarnation":"<approved lineage>"}
```

Exit status is zero on success, nonzero on failure. Errors are deliberately
opaque; no manifest, raw token, verifier, or serde diagnostic is printed.
The import never starts a worker/listener. A subsequent ordinary process start
(without the CLI flag) must verify and restore the file before serving.

## Explicit offline baseline import (not exact restoration)

The separate CLI, used alone, is:

```text
woven-server --managed-state-import-baseline
```

It uses the same complete managed environment, current private admin credential,
existing empty `0700` directory, bounded private stdin and exclusive writer lock.
It opens **no listeners**, starts no worker, and cannot replace an exact store,
previous baseline, corrupt store, pending/uncertain import, or unrelated files.
Approval is required in stdin, never inferred from selecting the CLI alone:

```json
{
  "version": 1,
  "nodeIncarnation": "<approved NEW 48-character lowercase-hex lineage>",
  "approval": "approve-new-lineage-with-unknown-retired-verifier-history-v1",
  "checkpoint": {
    "version": 1,
    "migrationBaseline": {
      "version": 1,
      "migrationId": "<approved unique migration record id>",
      "previousLineage": "<previous 48-character lowercase-hex lineage>",
      "closedScopeThrough": 100,
      "unknownRetiredVerifierHistory": true
    },
    "scopes": []
  }
}
```

This illustrates the shape, not an executable or complete production inventory.
Populate `scopes` with **every approved active product**, not just two examples,
and any known retained tombstones. Existing IDs, revisions, CCU, tick rate,
spatial bit definitions, and active client verifiers use the same DTO fields as
exact import. For an inventory scope whose historical last request is unknown,
set `lastMutation` to `{"kind":"migrationBaseline"}` rather than inventing it.
The initial active inventory may exceed ten products within real core limits;
normal startup restores all imported scopes automatically, without per-product
reprovisioning. Live entities, sessions' payload/world state and occupancy still
start empty; none is claimed as a world save.

Required metadata and safeguards:

- `version` is 1; `migrationId` is 1..128 **graphic ASCII** bytes (no whitespace).
  Host must assign a fresh approved migration-record identity; the local importer
  cannot authenticate the external record or certify completeness.
- `previousLineage` is 48 lowercase hex characters. The approved serving
  `nodeIncarnation` must be a **different** 48-character lowercase-hex value.
  Both import and later normal startup reject reused previous lineage.
- `closedScopeThrough` is a positive numeric JSON `u64` copied losslessly from
  the exact approved scope-registry high-water mark. An absent scope cannot be
  provisioned if **either** namespace or session ID is at/below that mark; mixed
  old/new pairs are fenced too. Imported active historical scopes remain usable.
  New allocations require both IDs above the fence.
- `unknownRetiredVerifierHistory` must explicitly be `true`. The fence does not
  recreate lost retired verifiers or prove their successful revocation/non-reuse.
  Known tombstones are still retained; fresh credentials for future allocations
  and acceptance of the unrecoverable-history limitation remain Host approval
  responsibilities.
- Historical PUT/PATCH/spatial retries at the imported revision are rejected for
  `migrationBaseline` scopes. The first real edit must be revision R+1; it records
  actual retry identity, which survives later restart. Baseline provenance/fence
  remain immutable through ordinary edits, new allocations and teardown.
- The `approval` field must equal the literal above. Missing/wrong approval,
  missing metadata, malformed/invalid inventory, or reused lineage fails without
  committing. The exact CLI/API rejects baseline metadata even without approval.
  Conversely, the baseline CLI/API rejects metadata-free exact manifests.

The privileged library entry point is distinct:

```rust,ignore
pub async fn import_managed_state_baseline(
    config: ManagedServerConfig,
    manifest: Vec<u8>,
) -> Result<ManagedStateImportResult, ServerError>;
// MANAGED_BASELINE_IMPORT_APPROVAL =
// "approve-new-lineage-with-unknown-retired-verifier-history-v1"
```

The approval field is import-only and is not stored. Validated metadata is stored
inside the checkpoint and included in the safe baseline success receipt:

```json
{
  "nodeIncarnation": "<approved new lineage>",
  "managedState": {
    "mode": "durable",
    "migrationBaseline": {
      "version": 1,
      "migrationId": "<approved migration record id>",
      "previousLineage": "<previous lineage>",
      "closedScopeThrough": "100",
      "unknownRetiredVerifierHistory": true
    }
  }
}
```

After ordinary startup, authenticated `GET /v1/node` includes that same
`managedState` object alongside serving `nodeIncarnation`, fresh `bootId`, and
existing capabilities. Note that the **status/receipt** uses a decimal **string**
for `closedScopeThrough`, unlike the checkpoint's numeric `u64`. Exact durable
startup reports `{"mode":"durable"}` without `migrationBaseline`; managed
startup without a state directory reports `{"mode":"ephemeral"}`. Status is a
safe immutable projection of metadata only after full core validation; no raw
checkpoint, token or verifier is returned. Corrupt/invalid state never reaches a
public listener or status response. No baseline import/rewrite HTTP endpoint is
exposed.

Host must compare all approved migration fields and the new lineage before
transactional adoption. A matching node status is evidence of the serving local
baseline, **not** approval to update Host bindings, bypass owner/generation/deletion
fences, deploy, or resume traffic. Those steps remain separately reviewed.

## Existing lost-state migration is a separate gate

The initial recovery discussed for Host registrations `jdharrison_com` (`4/4`)
and `prototypical` (`6/6`) is **not yet approved or executable as exact restoration**.
The reported Host records retain current configuration and encrypted credentials,
but completed pending mutations are removed and deleted credentials/tombstones
may no longer exist. Revision numbers and current spatial definitions do not
establish the exact historical last request. The scope registry is not a verifier
archive. Do not fabricate a syntactically valid checkpoint to fill these gaps.

If no complete authoritative checkpoint/archive exists, the separately approved
offline baseline mechanism above is now implemented locally. It is **not exact
restoration**, and its implementation does not approve a production migration:

- Use a new managed lineage, retaining the product generations, approved scope
  IDs/revisions/configuration and active credentials, rather than impersonating
  complete restoration under the old lineage.
- Represent unavailable pre-baseline mutation identity honestly; reject historical
  same-revision mutation replay instead of inventing a last request.
- Persist a compact retired-allocation fence from the exact approved registry
  high-water mark. Absent historical/mixed old/new IDs cannot be re-provisioned;
  imported active scopes remain allowed. Do not expand missing history into
  fictional verifier tombstones or treat the fence as successful revocation.
- Lost retired-verifier non-reuse history cannot be recovered from a scope fence.
  Either recover that archive, or explicitly approve a new security-history
  boundary with fresh generated credentials for future allocations. The active
  visitor credential need not rotate, but the limitation must be accepted honestly.
- Fence all Host writers/in-flight work; privately prepare a complete active
  inventory and resumable migration record; verify the serving baseline; then
  transactionally adopt only approved new-lineage bindings under generation,
  owner, account-deletion, capacity, configuration and credential fences.

Neither ordinary `/recover` nor direct Firestore binding edits implement this.
The local baseline mechanism, demo-only fenced Host operator library, and real
12-product loopback/Firebase-emulator migration/restart tests are implemented.
See `woven-host/services/control-api/MANAGED-BASELINE-MIGRATION.md` in the sibling
repository. No production Host adoption, import or live restoration has been performed. The next
approval must resolve
this missing-history boundary before deployment/migration, not merely approve an
environment-variable change.

## Reviewed migration sequence

1. Stop the old writer and fence external provisioning/mutations using the
   separately reviewed Host/ops procedure. This document performs none of that.
2. Privately reconstruct a **complete approved** checkpoint from authoritative
   Host/operator records. For the identified recovery, preserve scopes `4/4` and
   `6/6`, their actual client verifiers, revision-5 spatial definitions and exact
   last-mutation identities, and all known retired history. Do not replay them as
   new revision-1 PUTs. If authoritative records are incomplete, exact restoration
   must stop. Only the separate explicitly approved baseline path may establish a
   new history boundary; neither importer reconstructs lost secrets/history or
   certifies inventory completeness.
3. Prepare a new empty private state directory owned by the runtime user on the
   persistent VM disk. The importer may create its `managed.lock`; no unrelated
   files, checkpoint, or pending checkpoint may be present. Never erase an
   existing directory to make an import pass.
4. Stream the approved manifest privately into the offline CLI under that same
   user and normal managed configuration. Do not use argv/environment for
   verifier-bearing manifest data, shell tracing, log capture of stdin, or a
   publicly exposed endpoint. No runtime `POST /v1/state/bootstrap` exists.
5. Verify the safe receipt, then start ordinary durable managed mode. Privately
   verify lineage, a fresh `bootId`, baseline provenance when applicable, all
   inventoried scopes/revisions (not just `4/4` and `6/6`), spatial definitions,
   and client admission before independently approving traffic/provisioning.
6. Retain protected backups and the persistent volume. A checksum detects
   corruption, not malicious rewrites, rollback, storage loss or approval of the
   wrong records. This single-node store is not replicated or anti-rollback
   consensus. Do not run two copies of one lineage simultaneously on copied
   disks; the local lock cannot fence separate machines.

## Files and failure handling

- `managed.lock`: `0600`, held with an OS exclusive nonblocking lifetime lock.
  Do not delete/replace it while a writer or timed-out blocking operation exists.
- `checkpoint.json`: strict v1 envelope `{version:1,payload:<manifest>,sha256:...}`;
  checksum is lowercase SHA-256 of the compact serde-encoded typed payload.
- `checkpoint.pending`: fixed exclusive-create `0600` temporary file. A save
  writes the complete bounded envelope, fsyncs that file, atomically renames it
  over `checkpoint.json`, then fsyncs the pinned directory before ACK.
- Directories must be exactly `0700`; files exactly `0600`, regular, single-linked,
  and owned by the service user. File opens reject symlinks/nonregular files.
  Directory handles are pinned and accessed through `/proc/self/fd`, preventing
  path replacement from redirecting a later write.
- After a crash, a safe leftover pending file is discarded **only if the main
  checkpoint is present and passes schema/checksum verification**. It is never
  promoted as a recovery guess. A missing main checkpoint, unsafe temporary file,
  or partial/uncertain import requires offline inspection/recovery, not automatic
  empty initialization or retry. Never hand-edit checksums or remove tombstones.

Local coverage: `cargo test --locked -p woven-server --all-targets --all-features`.
Tests use only synthetic credentials, throwaway private directories, local
certificate-verified QUIC and bounded loopback HTTP. SIGKILL coverage proves
restart of acknowledged local writes, not physical power-loss behavior of an
unverified filesystem, disk or VM platform.
