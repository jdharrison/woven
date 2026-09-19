import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { randomBytes } from "node:crypto";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { homedir, tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { chromium } from "playwright";

const packageRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));
const wovenRoot = resolve(packageRoot, "../..");
const MAX_ITERATIONS = 10_000;
const MAX_RESPONSE_BYTES = 65_536;
const options = parseOptions(process.argv.slice(2));
const directory = await mkdtemp(join(tmpdir(), "woven-browser-e2e-"));
const certificateFile = join(directory, "cert.pem");
const privateKeyFile = join(directory, "key.pem");
const adminTokenFile = join(directory, "admin");
const adminToken = randomBytes(32).toString("hex");
const clientToken = randomBytes(32).toString("hex");
const namespaceId = "1";
const sessionId = "1";
const scopePath = `/v1/namespaces/${namespaceId}/sessions/${sessionId}`;

let woven;
let browser;
let httpServer;
let pageConfig;
let serverOutput = "";
let failureDiagnostic = "";
let completed = 0;

try {
  run("cargo", ["build", "--offline", "--locked", "-p", "woven-server"], wovenRoot, 180_000);
  run(
    "openssl",
    [
      "req",
      "-x509",
      "-newkey",
      "ec",
      "-pkeyopt",
      "ec_paramgen_curve:prime256v1",
      "-noenc",
      "-days",
      "7",
      "-config",
      "/dev/null",
      "-subj",
      "/CN=localhost",
      "-addext",
      "subjectAltName=DNS:localhost,IP:127.0.0.1",
      "-keyout",
      privateKeyFile,
      "-out",
      certificateFile,
    ],
    directory,
    10_000,
  );
  await Promise.all([
    chmod(privateKeyFile, 0o600),
    writeFile(adminTokenFile, adminToken, { mode: 0o600 }),
  ]);

  const bundle = await build({
    entryPoints: [join(packageRoot, "test/browser-smoke.ts")],
    bundle: true,
    format: "esm",
    platform: "browser",
    target: "es2022",
    write: false,
    sourcemap: false,
    logLevel: "silent",
  });
  assert.equal(bundle.outputFiles.length, 1);
  const browserJavaScript = bundle.outputFiles[0].text;

  httpServer = createServer((request, response) => {
    response.setHeader("Cache-Control", "no-store");
    if (request.url === "/bundle.js") {
      response.writeHead(200, { "Content-Type": "text/javascript; charset=utf-8" });
      response.end(browserJavaScript);
      return;
    }
    if (request.url === "/" && pageConfig) {
      const config = JSON.stringify(pageConfig).replaceAll("<", "\\u003c");
      response.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
      response.end(
        `<!doctype html><meta charset="utf-8"><title>Woven browser smoke</title>` +
          `<script>window.__WOVEN_BROWSER_CONFIG__=${config}</script>` +
          `<script type="module" src="/bundle.js"></script>`,
      );
      return;
    }
    response.writeHead(404).end();
  });
  await listen(httpServer);
  const httpAddress = httpServer.address();
  assert.ok(httpAddress && typeof httpAddress !== "string");
  const origin = `http://127.0.0.1:${httpAddress.port}`;

  woven = spawn(join(wovenRoot, "target/debug/woven-server"), ["--log-none"], {
    cwd: directory,
    detached: true,
    stdio: ["ignore", "pipe", "pipe"],
    env: {
      PATH: process.env.PATH,
      HOME: homedir(),
      WOVEN_MANAGED_QUIC: "1",
      WOVEN_QUIC_BIND: "127.0.0.1:0",
      WOVEN_MANAGEMENT_BIND: "127.0.0.1:0",
      WOVEN_ADMIN_BIND: "127.0.0.1:0",
      WOVEN_TLS_CERT_FILE: certificateFile,
      WOVEN_TLS_KEY_FILE: privateKeyFile,
      WOVEN_ADMIN_TOKEN_FILE: adminTokenFile,
      WOVEN_MANAGED_WEBTRANSPORT: "1",
      WOVEN_WEBTRANSPORT_BIND: "127.0.0.1:0",
      WOVEN_WEBTRANSPORT_PATH: "/webtransport",
      WOVEN_WEBTRANSPORT_ALLOWED_ORIGINS: origin,
    },
  });
  woven.stderr.on("data", (chunk) => rememberServerOutput(chunk));
  const addresses = await managedAddresses(woven);
  const adminUrl = `http://${addresses.admin}`;
  const node = await adminJson(adminUrl, "/v1/node", { method: "GET" });
  assert.equal(node.transports?.quic, true);
  assert.equal(node.transports?.webTransport?.enabled, true);
  assert.match(node.transports?.webTransport?.certificateSha256 ?? "", /^[0-9a-f]{64}$/);
  assert.equal(addresses.webTransportUrl.endsWith("/webtransport"), true);

  const provision = await adminRequest(adminUrl, scopePath, {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
      "Woven-Node-Incarnation": node.nodeIncarnation,
    },
    body: JSON.stringify({ revision: "1", allocatedCCU: 1, clientToken }),
  });
  assert.equal(provision.status, 201);
  await boundedBody(provision);

  browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();
  const started = performance.now();
  do {
    pageConfig = {
      url: addresses.webTransportUrl,
      certificateSha256: node.transports.webTransport.certificateSha256,
      token: clientToken,
      namespaceId,
      sessionId,
      iteration: completed + 1,
    };
    try {
      await page.goto(origin, { waitUntil: "load", timeout: 15_000 });
      await page.waitForFunction(() => window.__WOVEN_BROWSER_RESULT__ !== undefined, undefined, {
        timeout: 30_000,
      });
      const result = await page.evaluate(() => window.__WOVEN_BROWSER_RESULT__);
      assert.equal(result?.ok, true, result?.ok === false ? result.error : "missing browser result");
      assert.match(result.entityId, /^[1-9][0-9]*$/);
    } catch (error) {
      failureDiagnostic = await browserFailureDiagnostic({
        iteration: completed + 1,
        browser,
        context,
        managementUrl: `http://${addresses.management}`,
        adminUrl,
        scopePath,
        incarnation: node.nodeIncarnation,
        woven,
      });
      throw error;
    }
    await waitForActiveCcu(adminUrl, scopePath, node.nodeIncarnation, 0);
    await delay(100);
    completed += 1;
    if (completed % 25 === 0) {
      const gauges = await liveGauges(`http://${addresses.management}`);
      console.log(
        `browser WebTransport soak: ${completed} iterations passed; active connections=${gauges.connections}; active sessions=${gauges.sessions}`,
      );
    }
  } while (shouldContinue(options, completed, started));

  const remove = await adminRequest(adminUrl, scopePath, {
    method: "DELETE",
    headers: {
      "If-Match": '"1"',
      "Woven-Node-Incarnation": node.nodeIncarnation,
    },
  });
  assert.equal(remove.status, 204);
  await boundedBody(remove);
  console.log(
    `PASS: real Chromium TypeScript/WebTransport smoke completed ${completed} iteration${completed === 1 ? "" : "s"}`,
  );
} catch (error) {
  if (failureDiagnostic) console.error(failureDiagnostic);
  if (serverOutput) console.error(`Managed Woven output (sanitized):\n${serverOutput}`);
  throw error;
} finally {
  if (browser) await browser.close();
  if (httpServer) {
    httpServer.closeAllConnections();
    await new Promise((accept) => httpServer.close(accept));
  }
  if (woven) await stopGroup(woven);
  await rm(directory, { recursive: true, force: true });
}

function parseOptions(args) {
  let iterations = 1;
  let durationSeconds;
  let iterationsSpecified = false;
  for (const argument of args) {
    if (/^--iterations=[1-9][0-9]*$/.test(argument)) {
      iterations = Number(argument.slice("--iterations=".length));
      iterationsSpecified = true;
    } else if (/^--duration-seconds=[1-9][0-9]*$/.test(argument)) {
      durationSeconds = Number(argument.slice("--duration-seconds=".length));
    } else {
      throw new Error("usage: browser-e2e.mjs [--iterations=1..10000] [--duration-seconds=1..900]");
    }
  }
  if (
    iterations > MAX_ITERATIONS ||
    (durationSeconds !== undefined && durationSeconds > 900) ||
    (iterationsSpecified && durationSeconds !== undefined)
  ) {
    throw new Error("browser E2E bounds exceeded");
  }
  return { iterations, durationSeconds };
}

function shouldContinue({ iterations, durationSeconds }, completed, started) {
  if (completed >= MAX_ITERATIONS) return false;
  if (durationSeconds !== undefined) {
    return performance.now() - started < durationSeconds * 1_000;
  }
  return completed < iterations;
}

function run(command, args, cwd, timeout) {
  const result = spawnSync(command, args, { cwd, stdio: "inherit", timeout });
  if (result.status !== 0) throw new Error(`${command} failed`);
}

function listen(server) {
  return new Promise((accept, reject) => {
    server.once("error", reject);
    server.listen({ host: "127.0.0.1", port: 0, exclusive: true }, accept);
  });
}

function rememberServerOutput(chunk) {
  serverOutput = `${serverOutput}${chunk.toString("utf8")}`.slice(-8_192);
}

async function managedAddresses(child) {
  const lines = createInterface({ input: child.stdout });
  return Promise.race([
    new Promise((accept, reject) => {
      child.once("exit", (code, signal) => reject(new Error(`managed Woven exited: ${signal ?? code}`)));
      lines.on("line", (line) => {
        rememberServerOutput(`${line}\n`);
        const match = line.match(
          /^Woven managed QUIC ready at ([^;]+); WebTransport ready at (https:\/\/[^;]+); read-only management ([^;]+); authenticated loopback admin ([^;]+)$/,
        );
        if (match) {
          accept({
            quic: match[1],
            webTransportUrl: match[2],
            management: match[3],
            admin: match[4],
          });
        }
      });
    }),
    delay(15_000).then(() => {
      throw new Error("managed Woven startup timed out");
    }),
  ]);
}

async function adminRequest(adminUrl, path, init) {
  return fetch(`${adminUrl}${path}`, {
    ...init,
    redirect: "error",
    signal: AbortSignal.timeout(5_000),
    headers: {
      Authorization: `Bearer ${adminToken}`,
      ...(init.headers ?? {}),
    },
  });
}

async function adminJson(adminUrl, path, init) {
  const response = await adminRequest(adminUrl, path, init);
  assert.equal(response.ok, true);
  const bytes = await boundedResponseBytes(response);
  assert.ok(bytes.length > 0);
  return JSON.parse(new TextDecoder().decode(bytes));
}

async function boundedBody(response) {
  await boundedResponseBytes(response);
}

async function boundedResponseBytes(response) {
  const reader = response.body?.getReader();
  if (!reader) return new Uint8Array(0);
  const chunks = [];
  let length = 0;
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      if (length + value.byteLength > MAX_RESPONSE_BYTES) {
        await reader.cancel("response exceeded local E2E bound").catch(() => {});
        throw new Error("response exceeded local E2E bound");
      }
      chunks.push(value);
      length += value.byteLength;
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

async function waitForActiveCcu(adminUrl, path, incarnation, expected) {
  const deadline = performance.now() + 5_000;
  while (performance.now() < deadline) {
    const snapshot = await adminJson(adminUrl, path, {
      method: "GET",
      headers: { "Woven-Node-Incarnation": incarnation },
    });
    if (snapshot.admission?.activeCCU === expected) return;
    await delay(100);
  }
  throw new Error(`active CCU did not reach ${expected}`);
}

async function browserFailureDiagnostic({
  iteration,
  browser,
  context,
  managementUrl,
  adminUrl,
  scopePath,
  incarnation,
  woven,
}) {
  const details = [
    `iteration=${iteration}`,
    `browserConnected=${browser.isConnected()}`,
    `contextPages=${context.pages().length}`,
    `wovenAlive=${woven.exitCode === null && woven.signalCode === null}`,
  ];
  try {
    const response = await fetch(`${managementUrl}/healthz`, {
      redirect: "error",
      signal: AbortSignal.timeout(2_000),
    });
    await response.body?.cancel();
    details.push(`healthStatus=${response.status}`);
  } catch (error) {
    details.push(`health=${diagnosticErrorKind(error)}`);
  }
  try {
    const response = await fetch(`${managementUrl}/metrics`, {
      redirect: "error",
      signal: AbortSignal.timeout(2_000),
    });
    const body = new TextDecoder().decode(await boundedResponseBytes(response));
    if (!response.ok) throw new Error("metrics unavailable");
    details.push(`connectionsActive=${prometheusValue(body, "woven_connections_active")}`);
    details.push(`sessionsActive=${prometheusValue(body, "woven_sessions_active")}`);
  } catch (error) {
    details.push(`metrics=${diagnosticErrorKind(error)}`);
  }
  try {
    const snapshot = await adminJson(adminUrl, scopePath, {
      method: "GET",
      headers: { "Woven-Node-Incarnation": incarnation },
    });
    details.push(`activeCCU=${snapshot.admission?.activeCCU ?? "unknown"}`);
    details.push(
      `queueDepth=${snapshot.admission?.queueDepth ?? snapshot.admission?.queue_depth ?? "unknown"}`,
    );
  } catch (error) {
    details.push(`scopeSnapshot=${diagnosticErrorKind(error)}`);
  }
  return `Browser E2E failure diagnostics (safe): ${details.join(" ")}`;
}

function diagnosticErrorKind(error) {
  return error instanceof Error && error.name ? error.name : "unavailable";
}

function prometheusValue(body, name) {
  const match = body.match(new RegExp(`^${name} ([0-9]+)$`, "m"));
  return match?.[1] ?? "unknown";
}

async function liveGauges(managementUrl) {
  const response = await fetch(`${managementUrl}/metrics`, {
    redirect: "error",
    signal: AbortSignal.timeout(2_000),
  });
  const body = new TextDecoder().decode(await boundedResponseBytes(response));
  assert.equal(response.ok, true);
  return {
    connections: prometheusValue(body, "woven_connections_active"),
    sessions: prometheusValue(body, "woven_sessions_active"),
  };
}

async function stopGroup(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  try {
    process.kill(-child.pid, "SIGINT");
  } catch (error) {
    if (error.code !== "ESRCH") throw error;
  }
  await Promise.race([
    new Promise((accept) => child.once("exit", accept)),
    delay(5_000).then(() => "timeout"),
  ]);
  if (child.exitCode === null && child.signalCode === null) {
    try {
      process.kill(-child.pid, "SIGKILL");
    } catch (error) {
      if (error.code !== "ESRCH") throw error;
    }
    await new Promise((accept) => child.once("exit", accept));
  }
}
