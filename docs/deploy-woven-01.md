# Debian `woven-01`: exact-SHA local deployment

The local deploy tooling and [deployment workflow](../.github/workflows/deploy.yml)
are implemented. **Deployment remains disabled; the live node has not been restarted.**
Cloud identity/IAM staging is complete. Production activation still requires the
security/access migration below and publishing the workflow. Keep automation disabled until those
steps and the remaining bootstrap checks are complete.

## Staged on September 11, 2026

- GitHub repository `jdharrison/woven` has `WOVEN_DEPLOY_ENABLED=false` explicitly
  set. `WOVEN_SSH_HOST_KEY` contains the Ed25519 public key retrieved over the
  existing strictly host-verified SSH connection. No private key was uploaded.
- `/usr/local/sbin/deploy-woven` is installed root-owned with mode `0755`;
  `/etc/systemd/system/woven-server.service` is root-owned with mode `0644`.
- `/opt/woven` and `/opt/woven/releases` exist as root-owned `0755` directories.
  There is no current release or rollback seed yet.
- Systemd reports the unit loaded, **disabled and inactive**, with `MainPID=0`.
  Boot enablement and service startup were not performed.
- Transferred tooling was checksum-verified. All 34 hermetic tests passed on the
  actual Debian VM, including the condition regression test. Native unit
  verification caught and prompted correction
  of the executable condition to `ConditionFileIsExecutable`; verification then
  reported only the expected missing `/opt/woven/current/woven-server`. Full unit
  startup validation is still pending installation of a real release.
- The existing standalone PID `39490` still owned UDP `8081` and loopback TCP
  `8080` after staging. This is a recorded observation, not a PID to reuse blindly.
- After GCP reauthentication, the dedicated identity/IAM resources below were
  created and read back for verification. OS Login metadata, firewall rules, the
  attached VM service account, and runtime credentials remain unchanged. The
  workflow commits remain local and unpushed.

Staging copies are retained in `/home/this/woven-deploy-staging-ea78ea3`; this is
not the privileged invocation path. Recheck file checksums and all observations
before activation.

## Cloud identity staging (disabled)

Project: `signalweave-112358` (`932588991464`). The following resources were
created with approval to prepare automation without activating it:

| Resource | Staged configuration |
| --- | --- |
| Deployment service account | `woven-node-deploy@signalweave-112358.iam.gserviceaccount.com` |
| Runtime service account | `woven-node-runtime@signalweave-112358.iam.gserviceaccount.com`; not attached to a VM |
| WIF provider | `projects/932588991464/locations/global/workloadIdentityPools/github-pool/providers/woven-node-github`; **disabled** |
| Project lookup custom role | `projects/signalweave-112358/roles/wovenDeployProjectLookup`; only `compute.projects.get` and `resourcemanager.projects.get` |
| VM permission | `roles/compute.osLogin` for deployment SA on **woven-01 only**, not OS Admin Login |
| IAP permission | `roles/iap.tunnelResourceAccessor` for deployment SA, conditioned on `destination.ip == '10.128.0.2' && destination.port == 22` |
| Runtime act-as permission | Deployment SA has `roles/iam.serviceAccountUser` only on the new runtime SA, not the attached default Compute SA |
| Runtime project roles | `roles/logging.logWriter`, `roles/monitoring.metricWriter` for the existing Ops Agent use case; no Editor |

The new provider checks immutable repository ID `1350893583`, owner ID `4994852`,
repository `jdharrison/woven`, ref `refs/heads/main`, workflow ref
`jdharrison/woven/.github/workflows/deploy.yml@refs/heads/main`, and event
`workflow_run`. Its repository principal set has `roles/iam.workloadIdentityUser`
on the deployment SA only. No user-managed service-account keys were created.
The existing Woven Host provider `github-provider` was not modified.

IAP API was enabled. No new firewall allowance was added: the existing SSH rule
already permits TCP 22. This is not a claim that the VM firewall is fully hardened.
GitHub variables `WOVEN_WIF_PROVIDER` and `WOVEN_DEPLOY_SERVICE_ACCOUNT` now point
to the staged identities. `WOVEN_DEPLOY_ENABLED` remains **false**, independently
of the provider's disabled state.

### Remaining activation checklist

1. Obtain approval for the maintenance/access transition and publishing the local
   Woven commits. Host commits are separate and must not be pushed incidentally.
2. Preserve operator/recovery access before OS Login activation. Verify the real
   deployment account's OS Login POSIX username before installing its narrow
   sudoers rule; no new sudo permission has been installed yet.
3. Review the attached Ops Agent dependencies, then stop the VM, attach the staged
   runtime service account, and start it in the approved window. The external IP
   is ephemeral: plan address retention or certificate/client endpoint changes
   **before stopping**, since the existing TLS identity is tied to that IP.
4. Enable OS Login only after access preservation is verified; validate non-admin
   login and the wrapper-only sudo boundary without broadly granting roles.
5. Seed a verified rollback release and migrate the standalone process to the
   installed systemd unit. Verify startup and rollback before enabling boot start.
6. Publish and validate the workflow with the GitHub flag still false. Enable the
   WIF provider only for approved access testing; its permissions have been read
   back, but keyless authentication/IAP SSH have not been exercised end to end.
7. Only after acceptance, set `WOVEN_DEPLOY_ENABLED=true` and trigger successful CI
   on current main. Neither flipping that flag nor enabling the provider alone
   completes the unfinished VM bootstrap.

## Implemented workflow

After bootstrap, the only privileged deployment invocation is:

```sh
sudo -n /usr/local/sbin/deploy-woven <successful-main-head-sha>
```

Replace the angle-bracket argument with the actual **40 lowercase hexadecimal
characters**, not a branch, tag, abbreviated SHA, pull request merge SHA, or latest
`main`. The installed source is `ops/deploy/deploy-woven`. It accepts exactly one
argument and has no environment-based configuration or test switches. Do **not**
run `sudo /home/this/woven/ops/deploy/deploy-woven`: root must execute only the
reviewed, root-owned installed copy. Unit/script updates are manual reviewed
installation operations, not payloads executed from each deployed commit.

`deploy.yml` is a **latest-main reconciler**, not a deployment of the triggering
run's SHA:

1. `workflow_run` completion of `CI` on `main` triggers reconciliation, gated to
   trusted same-repository **push** runs. This includes failed or old completions;
   their conclusion is not the deployment decision.
2. Using `gh api`, it resolves current `main`, then queries `ci.yml` push runs for
   that **exact SHA** (`branch=main`, `event=push`, `head_sha`, `per_page=1`). It
   proceeds only if the latest matching run's conclusion is `success`; absent,
   pending, or failed latest CI means no deployment.
3. Concurrency group `production-woven-01` serializes execution with
   `cancel-in-progress: false`. Pending reconcilers may be superseded safely: the
   next one resolves newest main afresh. This is not a durable FIFO or a promise
   to deploy every successful SHA. The job has a 45-minute timeout.
4. Before **each SSH attempt**, it rechecks that main still equals the selected
   SHA and skips if superseded. It invokes the installed wrapper over IAP on
   `woven-01`, project `signalweave-112358`, zone `us-central1-f`. Only lock-busy
   **exit 75** is retried: at most **3 attempts**, with **20-second gaps**. Other
   errors fail immediately. A main change after the final check remains possible;
   the wrapper always uses the supplied SHA, never substitutes the fetched head.

The wrapper's nonblocking exclusive `flock` covers fetch, build, switch, health,
rollback, and cleanup. Success is **0**; other handled deployment failures are
**1**, not a busy/retry signal. It fetches public HTTPS as `this` under the lock;
the non-admin caller needs neither checkout access nor permission to run Git as
`this`. The wrapper checks main ancestry, not GitHub CI success or head equality.

### Repository variables and host-key pinning

| Variable | Required value |
| --- | --- |
| `WOVEN_DEPLOY_ENABLED` | Exactly `true` to admit jobs; unset/other values keep deployment disabled |
| `WOVEN_WIF_PROVIDER` | Approved deployment-specific WIF provider resource name |
| `WOVEN_DEPLOY_SERVICE_ACCOUNT` | Dedicated deployment service-account email |
| `WOVEN_SSH_HOST_KEY` | Independently verified Ed25519 public host key: `ssh-ed25519 <base64>`, without a comment or hostname |

**Disabled by default:** leave the enable flag unset/false until bootstrap, access,
rollback, and host-key verification are accepted. The staged WIF provider is also
disabled and must be enabled separately for approved keyless access testing.
The flag is a job-admission gate, **not live cancellation** of an already admitted
deployment. Disabling the provider does not revoke credentials already issued.

The workflow looks up the numeric VM instance ID and pins the public key under
`compute.<instance_id>` in gcloud's default `~/.ssh/google_compute_known_hosts`.
It uses native `--strict-host-key-checking=yes`, not a competing custom known-hosts
file or trust-on-first-use. SSH uses IAP, a ten-minute login-key expiry, a 20-second
connect timeout, and 15-second keepalives with four missed responses allowed.

### Security/access approval gate — production setup blocked

The scoped staging grants above are applied. The access/maintenance transition
still requires explicit approval:

- Use a **separate WIF provider**, restricted to `jdharrison/woven`, `main`, and
  `.github/workflows/deploy.yml`, and a dedicated deployment identity. Repository
  restriction alone is insufficient.
- Grant **non-admin OS Login only on this VM**, IAP tunnel access, and only the
  minimum compute lookup permissions needed by instance lookup/gcloud SSH. Its
  sole sudo permission is the root-owned `deploy-woven` wrapper with the validated
  SHA; no general sudo, systemctl, shell, or commands as `this`.
- Preserve and verify operator/recovery access **before enabling OS Login**:
  enabling it disables metadata-based SSH keys. Do not lock out the existing
  operator while introducing the automation identity.
- Per the reported VM inspection, the attached default VM service account has
  **`roles/editor` and `roles/cloudbuild.builds.builder`**, with **`cloud-platform`
  scopes**. Do **not** blindly grant `iam.serviceAccounts.actAs` on that account
  to make SSH work. VM-local access to its credentials also makes the broad
  attached identity a security concern despite non-admin login/scoped sudo.
- The **dedicated least-privilege runtime service account** is staged, not attached.
  Replacing the VM service account requires a reviewed **VM stop/start**, dependency
  assessment, IP/TLS continuity plan, and access-preservation plan. These remain
  required before production activation.

## Files and behavior

| Path | Ownership / purpose |
| --- | --- |
| `/home/this/woven` | Existing `this` checkout; never reset, cleaned, or overwritten |
| `/var/tmp/woven-build-*` | Temporary isolated detached worktree and target directory, owned by `this` |
| `/usr/local/sbin/deploy-woven` | Reviewed root-owned executable; parent directories must not be writable by `this` |
| `/etc/systemd/system/woven-server.service` | Reviewed root-owned unit |
| `/opt/woven/releases/<sha>/woven-server` | Root-owned copied release executable, independent of checkout and Cargo target |
| `/opt/woven/current` | Relative symlink to the running release |
| `/opt/woven/previous` | Relative symlink to the previous successful release |
| `/opt/woven/deploy.lock` | Root-owned serialization lock; never replace it during a deployment |

After root/path preflight and lock acquisition, the script runs the following
fixed fetch as `this` in `/home/this/woven`, with a **120-second timeout**:

```sh
git fetch --no-tags https://github.com/jdharrison/woven.git +refs/heads/main:refs/remotes/origin/main
```

Git hooks are disabled with `core.hooksPath=/dev/null`; credential helpers and
askpass are disabled for the fetch, and terminal prompting is disabled. Root
launches `runuser`, not Git itself. The URL/refspec are not caller-configurable.
This updates Git objects/tracking metadata only, without reset or checkout of the
primary working tree. A fetch failure/timeout fails closed before building or
switching the live release. HTTPS access to the public repository is required even
when reusing a retained release; no SSH Git remote or caller credentials are used.

The primary checkout must have no tracked changes or untracked files (normal Git
ignored build outputs are allowed). A detached `git worktree` is created for the
exact commit with checkout hooks disabled; its HEAD and cleanliness are checked.
Git and all Cargo/build-script execution run as `this`, with a minimal environment.
The build uses an isolated target directory and this exact Cargo command:

```sh
/home/this/.cargo/bin/cargo build --locked --release -p woven-server
```

The build deadline is **1,800 seconds**; command timeout/termination kills its
process group. Other commands have individual deadlines (Git setup at most 120
seconds, cleanup 60 seconds, systemctl 45 seconds). Dependencies/toolchains may
require downloads on the host; provision caches beforehand if builds must be
offline. The script does not inspect, copy, or log credential files. Command output,
including build output, is suppressed to avoid accidentally recording secrets;
on build failure reproduce the build **unprivileged** and inspect locally using
normal secret-handling precautions. Only controlled status/error messages are
printed. This is not a sandbox against malicious Rust/build scripts: trust CI
commits and the `this` account/toolchain. That account also owns runtime secrets.

Before transferring scratch-directory ownership to `this`, the root process opens
and retains its directory descriptor. Absolute path components, including
`/var/tmp` and the new scratch directory, are opened with `O_DIRECTORY|O_NOFOLLOW`.
After the build, `target` and `release` are each opened relative to that anchored
descriptor with the same flags; the final `woven-server` is opened with
`O_NOFOLLOW|O_NONBLOCK` and must be a regular file. Intermediate symlinks are never
followed, including after a scratch pathname replacement.

The copy is limited to the initial `fstat` length (nonzero and at most **512 MiB**),
using bounded chunks and a **60-second monotonic deadline** checked between I/O
operations. Early EOF, extra bytes, or changed size/mtime/ctime reject the artifact.
The deadline is cooperative: it does not interrupt a kernel I/O operation stalled
on a failed filesystem. This defense does not make the trusted `this` account or
its build scripts an adversarial sandbox or prove build provenance.

The copy is written into a private root-owned `.stage-*` directory under releases;
only the completed, validated, flushed executable is promoted by a directory
rename to its SHA release. Failure or cancellation removes the private stage.
A hard crash may leave a `.stage-*` directory for reviewed manual cleanup.
The `current` pointer is changed using same-directory `os.replace` of a temporary
symlink. This is an atomic pathname switch, **not** zero-downtime deployment: the
single service is restarted and clients disconnect. The previous pointer is
recorded before switching. A release already referenced by current/previous can
be reused without rebuilding; a same-current-SHA call checks health without
restarting. An unhealthy same-SHA retry fails for operator attention.

Failure after switching restores the old current pointer, resets systemd's failed
state, restarts the prior binary, and repeats health checks. A failed first install
**without a seeded current release** has no rollback binary: it removes `current`
and stops only the managed unit. With a reviewed seeded current release, the first
deployment uses that existing binary as its rollback target.
Rollback failure is explicitly nonzero and requires manual intervention. Successful
rollback still returns failure for the rejected deployment. **Only confirmed
activation or confirmed recovery** prunes SHA-named release directories to at most
current and previous. There is no unconditional preflight/finally pruning in `main`.
On rollback failure or another uncertain outcome, **all releases are retained**,
including the older previous A, prior current B, and rejected candidate C. The
symlink pointers alone may no longer describe every useful recovery binary.
Pause automatic deployment and inspect all retained releases before any further
invocation; a later confirmed successful deployment can prune unreferenced ones.
A candidate temporarily makes three directories while a transaction is in progress;
uncertain failures can retain more than two until reviewed recovery. Unrecognized
directory names are never recursively removed by release pruning.

SIGTERM/SIGINT/SIGHUP raise a distinct cancellation exception that health checks do
not catch or retry. They trigger cleanup and, after switching, rollback; all three
signals are ignored during the recovery attempt so it can complete. SIGKILL, kernel/host
failure, or power loss cannot run recovery; symlink renames are atomic but the
whole deployment is not a crash-durable transaction. A crash can leave a third
release or worktree. Before retrying, inspect pointers, all retained releases, and
managed process state; pruning resumes only after confirmed activation or recovery. Remove only a confirmed
stale `woven-build-*` worktree via `git worktree remove` as `this`; never blindly
clean the primary checkout or delete a possibly active build directory.

## What health means

Each check requires all of:

- `woven-server.service` is active with a nonzero `MainPID`;
- `/proc/<MainPID>/exe` is the requested release binary;
- `ss` shows that same PID owning UDP `0.0.0.0:8081` and TCP `127.0.0.1:8080`;
- a proxy-free, configuration-free, bounded HTTP GET of
  `http://127.0.0.1:8080/metrics` succeeds. Response bodies are discarded.

Three consecutive checks must pass within 12 attempts, with a one-second interval.
Each of the five external probes has a three-second ceiling (curl additionally has
one-second connect and two-second total deadlines), so one health phase is bounded
by approximately **192 seconds** plus scheduling overhead. Rollback has its own
bounded health phase. This does **not** demonstrate TLS trust/expiry, authenticated
WVN1 QUIC handshake, authorization, routing/delivery, external reachability, or
capacity. HTTP metrics or systemd active state alone are not data-plane readiness.
An independently approved protocol-level test is needed for those claims.

## Reviewed manual bootstrap (installation staged; activation pending)

Use an approved maintenance window. Debian needs Python 3, systemd, Git, util-linux
(`runuser`), iproute2 (`ss`), curl, and the existing Rust toolchain. Verify executable
locations used by the script on the actual host. Confirm `this` and group `this`
exist, the source checkout and Cargo installation belong to that account, and
`/opt`, `/usr/local/sbin`, and installed deployment paths are root-controlled.
Do not start anything yet.

### Verify existing credentials without displaying contents

Per the operator's VM inspection, these filenames were verified on the inspected
`woven-01` VM: all three files are owned by `this` with mode `0600`, and their
containing `woven-cloud-test` directory has mode `0700`. The unit uses these paths.
No credential contents were read by this implementation:

```text
/home/this/.local/share/woven-cloud-test/chain.pem
/home/this/.local/share/woven-cloud-test/key.pem
/home/this/.local/share/woven-cloud-test/token
```

Use file metadata/access checks, not `cat`, shell tracing, or token output. Key and
token files must deny all group/other permissions and be readable by `this`; parent
directories must permit traversal by that account. No secrets belong in the unit,
sudoers, workflow, arguments, or documentation. Do not create, rotate, relocate, or
change permissions on existing secrets without separate review/approval.

### Explicitly migrate the standalone server

Before the first deployment, identify who owns the existing TCP 8080 / UDP 8081
listeners and the exact standalone process or supervisor. Record its launch
configuration and an approved way to restore it. During the window, disable its
specific autostart mechanism and gracefully stop **that identified process using
its existing supervisor or an individually verified PID**. Confirm both ports are
free before starting the unit. Do not use `pkill`, `killall`, broad port-killing
commands, or kill an unknown PID. The deployment script never performs this
migration. Its first-deploy rollback cannot resurrect the old standalone process;
that restoration is a separate manual procedure. Do not let two supervisors fight
over the ports.

### Optional: seed the standalone binary as initial current

For this migration, the operator plans to seed the existing standalone executable
before the first automated deployment. This is a **manual bootstrap option**, not
an extra deploy-script argument and not an action performed here:

1. Establish the standalone binary's actual full source commit SHA and provenance;
   do not invent a SHA, label an unknown build as main, or use the binary's SHA-256
   as a commit ID. If provenance cannot be established, retain the separate manual
   restoration plan rather than fabricating a release identity.
2. As an authorized administrator, copy the verified executable (not a symlink to
   a mutable Cargo target) into `/opt/woven/releases/<verified-commit-sha>/woven-server`.
   Make the release directory and binary root-owned and mode `0755`, matching the
   normal release layout. Keep the standalone restoration copy until migration is
   accepted. Do not overwrite an existing release or pointer without review.
3. Before admitting CI deployments, seed `/opt/woven/current` as a relative symlink
   to `releases/<verified-commit-sha>` using a same-directory atomic rename; leave
   `previous` absent for this initial seed. Coordinate under the deployment lock if
   any deploy caller is already enabled. Do not leave the seed unreferenced: normal
   deployment cleanup removes unreferenced SHA release directories.
4. After explicitly stopping the identified standalone process and installing the
   reviewed unit, start **only** `woven-server.service` on the seeded release as
   part of the approved migration and verify the process/listener/metrics checks.
   Confirm that the binary supports the unit's arguments, environment and secret
   paths before relying on it for rollback. Only then allow the first CI deployment.

A failed first candidate can now restore the seed and restart it under systemd;
this does not restart its former standalone supervisor. Automatic rollback reuses
that copied binary without a fetch or rebuild. Later explicit rollback through the
SHA entry point still requires successful HTTPS fetch and current main ancestry.
If a CI SHA equals the seed SHA, the script reuses the seed rather than rebuilding,
so truthful provenance is essential.

### Install reviewed copies

After approval, an administrator must review existing files/drop-ins/pointers,
create root-owned mode-`0755` `/opt/woven` and `/opt/woven/releases`, and install the
reviewed script as root-owned mode-`0755` `/usr/local/sbin/deploy-woven` and unit as
root-owned mode-`0644` `/etc/systemd/system/woven-server.service`. Verify the unit,
reload systemd, and enable boot startup as a separate manual bootstrap. File
installation and daemon reload have been performed as recorded above; boot
startup remains disabled. Recheck existing installed copies before replacing them.

Enabling sets boot startup but does **not** start the unit. It has an executable
condition so boot before the first release does not try to launch a missing binary.
After standalone migration, run the first approved successful-main SHA deployment
using the invocation above. No `--now` bootstrap and no arbitrary process killing.

The unit uses `User=this`, explicit remote QUIC mode/address and management loopback,
startup/stop timeouts, `Restart=on-failure` with five starts per 120 seconds and a
five-second delay, read-only filesystem/home, no capabilities or privilege gain,
private temporary/device namespaces, and restricted address families. It sends
SIGINT for graceful shutdown (the server listens for Ctrl-C), with bounded stop and
control-group cleanup. Automatic restart is deliberately rate-limited, not an
infinite crash loop. Server stdout/stderr goes to the journal; the application must
continue to avoid credential logging. Management HTTP remains unauthenticated and
must never be exposed or publicly proxied. This single-static-token remote mode is
not production multi-tenant authentication or remote WebTransport.

An authorized administrator must separately review the minimal sudoers entry via
`visudo` for the **actual OS Login POSIX username of the dedicated service account**.
Do not substitute `this`, guess the generated username, or grant OS Login admin
access. Grant that principal only root execution of `/usr/local/sbin/deploy-woven`
with arguments handled by the script's exact single-SHA validator. Do not grant
sudo `runuser`, Git, Cargo, systemctl, a shell, arbitrary paths, or commands as `this`.
If a sudoers argument wildcard is used, it is safe only with that validation and
root ownership; it is not shell execution permission. Do not allow environment
preservation/SETENV. This authorizes main-ancestry deployments, not just CI-passing
head commits: protect the caller's identity and enforce CI success/head checks in
orchestration. Never permit sudo execution of files writable by `this`.

## Manual rollback / recovery

For a healthy deployment you deliberately want to undo, read the **symlink target**
of `/opt/woven/previous` (not any credential file), extract its full SHA, then invoke
`sudo -n /usr/local/sbin/deploy-woven` with that literal SHA. This serializes with
other deployments, reuses the previous binary, validates it, and retains the
superseded binary as the new previous. The HTTPS fetch, clean-source and
main-ancestry guards still apply, so this entry point is not an offline recovery
command. Disable new workflow admission and confirm any admitted deployment has
finished before an approved manual rollback; the enable flag does not cancel an
active job. Do not alter refs/work to bypass a rejection.

If automated rollback failed or the checkout guards cannot be met, pause the
external deployment queue and perform a reviewed maintenance recovery under the
same exclusive lock. Inspect current/previous targets, executable ownership, unit
state and listener ownership; restore only a verified retained release, reset the
unit start limit, restart only `woven-server.service`, and repeat the checks above.
Do not delete the lock to break contention, read secrets for diagnosis, or stop
unrelated services. A failed initial deployment may instead need the independently
recorded standalone restoration procedure.

## Local validation

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s ops/deploy -p 'test_*.py' -v
```

Tests use temporary directories and mocked commands, systemd, and health outcomes;
they do not require root, credentials, network access, or a running systemd. Coverage
includes exact argument validation, root/path preflight, fixed unprivileged fetch
arguments/deadline, fetch lock coverage and fail-closed behavior, dirty-source/object
rejection, exact isolated build arguments/deadline, distinct lock-busy exit code,
successful switch/retention, restart/health failure rollback, failed-rollback release
retention through `main`, failed initial deployment, seeded-first-deployment rollback,
idempotence, pointer validation, cancellation at every health probe, rollback signal
handling, and bounded listener-aware probes. Artifact tests cover intermediate/final
symlinks, descriptor anchoring, invalid file types/sizes, growth/truncation/mutation,
staging cleanup, and copy deadlines.
Actual Debian unit hardening, filesystem permissions, Cargo build, boot, and
signal/crash behavior require a separately approved host rehearsal.

The main agent will run the repository-wide mandated checks from `AGENTS.md`:
`cargo fmt --all -- --check`, locked workspace/all-targets/all-features clippy with
`-D warnings`, the matching locked test suite, locked workspace docs with
`RUSTDOCFLAGS=-Dwarnings`, and `cargo audit`. This local tooling change does not
replace those checks or claim to have run a real deployment.
