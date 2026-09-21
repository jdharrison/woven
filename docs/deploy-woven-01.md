# `woven-01` production deployment

This runbook covers the managed Woven data plane on Compute Engine. The external
client contract uses one stable hostname and does not expose VM, region, resource,
or fabric names:

```text
Native QUIC:  quic://api.woven.host:4433
WebTransport: https://api.woven.host:4434/webtransport
```

Both transports terminate TLS directly in `woven-server`. Google HTTPS load
balancers and Certificate Manager are not in this path because they cannot pass
Woven's native QUIC protocol through unchanged or export a Google-managed private
key to the node. The node uses a publicly trusted ACME certificate for exactly
`api.woven.host`; no wildcard or private CA is used.

## Current production state

Project and instance:

```text
Project:       signalweave-112358
Instance:      woven-01
Zone:          us-central1-f
Reserved IPv4: 104.198.144.33 (woven-api-ip, us-central1)
```

The managed service is deliberately stopped and disabled until DNS, public TLS,
firewall changes, and the reviewed unit are installed. Deployment automation also
remains disabled through `WOVEN_DEPLOY_ENABLED=false` and the disabled
`woven-node-github` Workload Identity provider.

The VM contains a root-owned exact-SHA deployment wrapper at
`/usr/local/sbin/deploy-woven`, a root-owned systemd unit, and the seeded release
for commit `73eb431aedaf894feec09c604793f35528f59a4f`. Re-verify this state before
activation rather than relying on this document as live inventory.

## Network boundary

The reviewed unit requires these listeners:

| Listener | Exposure | Purpose |
| --- | --- | --- |
| UDP `0.0.0.0:4433` | Public | Native WVN1 QUIC |
| UDP `0.0.0.0:4434` | Public | Browser WebTransport |
| TCP `127.0.0.1:8080` | Loopback only | Read-only telemetry |
| TCP `127.0.0.1:8083` | Loopback only | Authenticated managed admin API |

The browser endpoint permits only these exact origins:

```text
https://woven.host
https://signalweave-112358.web.app
```

Certificate hostname coverage does not authorize browser origins. Do not add
arbitrary origins, expose either loopback listener, or infer client endpoints from
the management address.

The production firewall should permit public UDP `4433` and `4434`. ACME HTTP-01
also requires public TCP `80`; Certbot binds it only while issuing or renewing.
The old workstation-scoped UDP `8081`/`8082` rule must be removed after the new
rules are verified. No public TCP rule is needed for telemetry or admin.

## DNS and public certificate

Create this unproxied DNS record at the authoritative provider:

```text
api.woven.host.  A  104.198.144.33
```

Verify the answer from multiple public resolvers before requesting a certificate.
Do not request the certificate against an unpropagated record.

Install Certbot from Debian packages and use its standalone HTTP-01 authenticator.
The ACME account email is `this@jacobdharrison.com`. The reviewed certificate hook
is `ops/deploy/install-woven-tls`; install it root-owned and mode `0755` at:

```text
/usr/local/sbin/install-woven-tls
```

Prepare root-controlled destinations before issuance:

```text
/etc/woven/tls/releases/
/etc/woven/credentials/admin-token
```

`/etc/woven`, `tls`, `tls/releases`, and `credentials` must be real root-owned,
`this`-group directories with mode `0750`; `/etc` must remain traversable. The hook
fails closed if the service account cannot traverse every configured parent.
Certificate releases contain regular `root:this` mode-`0640` files and are selected
through the atomic `/etc/woven/tls/current` symlink. The admin token is also
root-owned and readable by the service group. Never print token or private-key
contents.

The hook accepts material only from Certbot's fixed
`/etc/letsencrypt/archive/api.woven.host` lineage. It requires exactly one DNS SAN,
`api.woven.host`, at least fourteen days of remaining validity, and a matching
private key. It synchronizes both files and the release directory before atomically
switching and synchronizing one symlink, so process or host interruption cannot
publish a mixed or non-durable certificate/key pair. If Woven is active, it
requires three complete PID/listener/metrics health observations
after restart; failure atomically restores and verifies the previous release.
Uncertain rollback retains both complete releases for operator recovery. If Woven
is inactive, the hook installs the pair without starting it.

Issue the single-name certificate only after DNS and TCP `80` are externally
reachable:

```sh
sudo certbot certonly --standalone \
  --preferred-challenges http \
  --domain api.woven.host \
  --email this@jacobdharrison.com \
  --agree-tos \
  --no-eff-email \
  --deploy-hook /usr/local/sbin/install-woven-tls
```

After issuance, run a bounded dry-run renewal and verify that the hook leaves the
service healthy:

```sh
sudo certbot renew --dry-run --run-deploy-hooks
```

Keep Certbot's systemd renewal timer enabled. Do not copy certificates through a
user workspace, commit them, or replace this with a self-signed fallback.

## Managed credentials

The admin credential is independent of every client/server token. It authorizes
only the loopback admin API and must never be accepted by QUIC, returned to clients,
placed in the systemd unit, or stored in the repository. Generate it once with at
least 32 random bytes, write it directly to
`/etc/woven/credentials/admin-token`, and set owner `root:this`, mode `0640`.

Host-managed client tokens are generated per provisioned scope and stored by Woven
only in memory. Woven Host persists only generation-bound AES-GCM ciphertext. A
client must present the token using managed Bearer admission and join the numeric
namespace/session provisioned for its registered product. Product IDs are not used
as Woven namespace IDs, and guessing a product ID does not grant admission.

Production Host integration additionally needs a private, authenticated path from
Cloud Run to TCP `8083` plus protected mounts for the admin token and Host credential
encryption key. Do not make `8083` public as a shortcut. Until that path and Host
environment are configured, the portal can retain registrations but cannot safely
provision this node.

## Reviewed service installation

Repository files:

```text
ops/deploy/woven-server.service
ops/deploy/deploy-woven
ops/deploy/install-woven-tls
```

Installed files must be root-owned and not writable by the service account:

```text
/etc/systemd/system/woven-server.service  0644
/usr/local/sbin/deploy-woven               0755
/usr/local/sbin/install-woven-tls          0755
```

Run `systemd-analyze verify` and `systemctl daemon-reload` after installing the
reviewed unit. `ProtectHome=true` prevents the runtime from reading home-directory
material. `/etc/woven` is explicitly read-only to the service, and the process has
no Linux capabilities or privilege escalation.

Do not enable or start the unit until its executable, complete TLS pair, and admin
token all exist. Activation should use a reviewed maintenance window:

1. Verify DNS, certificate hostname/expiry, file ownership, and firewall rules.
2. Verify `/opt/woven/current/woven-server` is the expected root-owned executable.
3. Start the unit and require every listener plus loopback metrics to pass.
4. Run external native QUIC and WebTransport protocol checks.
5. Enable boot startup only after the protocol checks pass.

## Exact-SHA deployment

The only privileged release invocation is:

```sh
sudo -n /usr/local/sbin/deploy-woven <40-character-successful-main-SHA>
```

The installed wrapper—not a checkout copy—must be executed. It validates one full
lowercase SHA, serializes the complete deployment with `flock`, fetches the public
`main` ref without credentials, checks ancestry and a clean source checkout, builds
in an isolated detached worktree as `this`, and copies a bounded regular artifact
into a root-owned release directory.

Activation atomically switches `/opt/woven/current`, restarts the unit, and requires
three consecutive observations of:

- the expected release executable as systemd `MainPID`;
- UDP `0.0.0.0:4433` and `0.0.0.0:4434` owned by that PID;
- TCP `127.0.0.1:8080` and `127.0.0.1:8083` owned by that PID;
- a bounded successful `http://127.0.0.1:8080/metrics` request.

The wrapper reloads systemd before resetting a failed unit so a never-started unit
cannot be garbage-collected during the bounded build. Candidate failure restores
the prior release and repeats health checks. Rollback failure is nonzero and retains
all release artifacts for operator recovery.

These checks prove process/listener/metrics readiness only. They do not prove public
TLS, authentication, routing, delivery, origin policy, or external reachability.
A managed protocol smoke test and soak remain separate release gates.

## Deployment automation

The workflow `.github/workflows/deploy.yml` is a latest-successful-main reconciler.
It uses a deployment-specific Workload Identity provider, a dedicated deployment
service account, IAP SSH with a pinned host key, and the installed exact-SHA wrapper.
Only lock-busy exit `75` is retried.

Keep both activation controls disabled until manual production validation passes:

- GitHub variable `WOVEN_DEPLOY_ENABLED=false`;
- WIF provider `woven-node-github` disabled.

The VM still uses the broad default Compute service account. Moving to
`woven-node-runtime@signalweave-112358.iam.gserviceaccount.com` requires a reviewed
stop/start and access-preservation window. Do not activate deployment automation or
attach identities merely to complete TLS setup.

## Production acceptance

Before calling the node production-ready:

1. Confirm public DNS and system/browser trust for `api.woven.host`.
2. Confirm UDP `4433` and `4434` externally and remove UDP `8081`/`8082` exposure.
3. Provision a disposable managed scope through the authenticated loopback admin API.
4. Run a 10-second native managed smoke test using the system CA bundle.
5. Run a 600-second managed native soak at the approved bounded rate.
6. Require every sent publish to be echoed, with zero errors or disconnects.
7. Confirm active CCU returns to zero and scope deletion succeeds.
8. Run the real browser WebTransport E2E against the production hostname and allowed origin.
9. Configure and test the private Woven Host management path.
10. Only then enable deployment automation and publish release artifacts in dependency order.
