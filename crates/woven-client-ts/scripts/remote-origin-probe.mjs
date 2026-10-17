import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { fileURLToPath } from "node:url";

const HELP = `Usage: node scripts/remote-origin-probe.mjs [--inspect | --run] [--endpoint=https://HOST:PORT/webtransport]
Default/--inspect: print the plan only; no browser, listeners, DNS, or network.
--run REQUIRES --endpoint and prior operator approval for that exact target.
Real Documents: http://127.0.0.1:8000, http://localhost:8000,
  https://woven.host, https://jdharrison.com; negative: http://127.0.0.1:8001.
One WebTransport attempt per origin, no retries, ready then close; total <90 seconds.
No credentials, bootstrap, WVN1 handshake/authentication, admission, streams, or publications.
A rejected transport handshake alone cannot identify an origin-policy rejection.`;

export const LIMITS = Object.freeze({
  overallMs: 89_000, workMs: 82_000, cleanupMs: 2_000,
  navigationMs: 6_000, readyMs: 4_000, closeMs: 500, documentRequests: 4,
});
export const ORIGIN_CASES = Object.freeze([
  { origin: "http://127.0.0.1:8000", expected: "ready" },
  { origin: "http://localhost:8000", expected: "ready" },
  { origin: "https://woven.host", expected: "ready" },
  { origin: "https://jdharrison.com", expected: "ready" },
  { origin: "http://127.0.0.1:8001", expected: "rejected" },
].map(Object.freeze));

function invalid() { throw new Error("invalid-options"); }
function endpointUrl(value) {
  if (typeof value !== "string" || value.length > 512 || /[\s\\?#]/u.test(value)) invalid();
  const url = new URL(value);
  if (!value.startsWith("https://") || url.username || url.password || url.pathname !== "/webtransport" ||
      url.href !== value || !url.hostname) invalid();
  return url.href;
}

export function parseOptions(args) {
  if (args.length > 3) invalid();
  const seen = new Set();
  let endpoint;
  for (const arg of args) {
    const key = arg.split("=", 1)[0];
    if (seen.has(key)) invalid();
    seen.add(key);
    if (key === "--endpoint" && arg.startsWith("--endpoint=")) endpoint = endpointUrl(arg.slice(11));
    else if (!["--help", "--run", "--inspect"].includes(arg)) invalid();
  }
  if (seen.has("--run") && seen.has("--inspect")) invalid();
  if (seen.has("--run") && endpoint === undefined) invalid();
  return { help: seen.has("--help"), run: seen.has("--run"), endpoint: endpoint ?? null };
}

export function unsafeLoggingEnvironment(env, execArgv = []) {
  return ["DEBUG", "PWDEBUG", "SSLKEYLOGFILE", "NODE_DEBUG", "NODE_DEBUG_NATIVE", "NODE_OPTIONS", "CHROME_LOG_FILE"]
    .some((key) => Boolean(env[key])) || execArgv.some((arg) => /^--(?:tls-keylog|inspect[^=]*|debug[^=]*)(?:=|$)/u.test(arg));
}

function bounded(promise, ms) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_resolve, reject) => { timer = setTimeout(() => reject(new Error("operation-deadline")), ms); }),
  ]).finally(() => clearTimeout(timer));
}

/** Only continue real main-document GETs. Never fulfill a document or modify Origin/CSP/TLS. */
export async function loadOriginDocument(page, origin, ownershipMarker) {
  const cdp = await page.context().newCDPSession(page);
  await cdp.send("Emulation.setScriptExecutionDisabled", { value: true });
  let navigating = true;
  let documentRequests = 0;
  await page.context().route("**/*", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    if (navigating && request.isNavigationRequest() && request.frame() === page.mainFrame() &&
        request.method() === "GET" && url.href === `${origin}/` && !url.username && !url.password && !url.search && !url.hash &&
        documentRequests++ < LIMITS.documentRequests) {
      await route.continue();
    } else {
      await route.abort("blockedbyclient");
    }
  });
  let response;
  try {
    response = await page.goto(`${origin}/`, { waitUntil: "domcontentloaded", timeout: LIMITS.navigationMs });
  } finally { navigating = false; }
  await cdp.send("Page.stopLoading");
  const headers = response?.headers();
  const url = new URL(page.url());
  if (!response?.ok() || !headers["content-type"]?.toLowerCase().startsWith("text/html") ||
      url.href !== `${origin}/` || url.search || url.hash ||
      (ownershipMarker !== undefined && headers["x-woven-origin-probe"] !== ownershipMarker)) {
    throw new Error("document-load-failed");
  }
  const inspected = await cdp.send("Runtime.evaluate", {
    expression: "({origin: globalThis.origin, locationOrigin: location.origin, secureContext: isSecureContext, webTransportAvailable: typeof WebTransport === 'function'})",
    returnByValue: true,
  });
  const evidence = inspected.result?.value;
  if (inspected.exceptionDetails || evidence?.origin !== origin || evidence.locationOrigin !== origin) {
    throw new Error("document-origin-mismatch");
  }
  // Runtime.evaluate can run the operator's code while page script execution stays disabled.
  // Never re-enable it: no site scripts, handlers, bootstrap, or autoconnect can run.
  return {
    cdp,
    document: {
      origin, httpStatus: response.status(), secureContext: evidence.secureContext === true,
      webTransportAvailable: evidence.webTransportAvailable === true,
      enforcedCspPresent: headers["content-security-policy"] !== undefined,
      reportOnlyCspPresent: headers["content-security-policy-report-only"] !== undefined,
    },
  };
}

/** Self-contained for CDP serialization and pure mocked tests. No Woven client/bundle is loaded. */
export async function probeTransport(config, runtime = globalThis) {
  let url;
  try { url = new URL(config.endpoint); } catch { return { outcome: "invalid-config", attempts: 0, close: "not-opened" }; }
  if (url.protocol !== "https:" || url.username || url.password || url.search || url.hash || url.pathname !== "/webtransport" ||
      !Number.isInteger(config.readyMs) || config.readyMs < 1 || config.readyMs > 4_000 ||
      !Number.isInteger(config.closeMs) || config.closeMs < 1 || config.closeMs > 500) {
    return { outcome: "invalid-config", attempts: 0, close: "not-opened" };
  }
  const result = { outcome: "document-origin-mismatch", attempts: 0, close: "not-opened", cspViolationObserved: false };
  if (runtime.origin !== config.origin || runtime.location.origin !== config.origin) return result;
  if (!runtime.isSecureContext) return { ...result, outcome: "insecure-context" };
  if (typeof runtime.WebTransport !== "function") return { ...result, outcome: "webtransport-unavailable" };
  const onViolation = (event) => {
    if (event.disposition === "enforce" && ["connect-src", "default-src"].includes(event.effectiveDirective)) {
      result.cspViolationObserved = true;
    }
  };
  const wait = async (promise, ms, timeoutValue) => {
    let timer;
    try {
      return await Promise.race([
        promise,
        new Promise((resolve) => { timer = runtime.setTimeout(() => resolve(timeoutValue), ms); }),
      ]);
    } finally { runtime.clearTimeout(timer); }
  };
  let transport;
  let closed;
  runtime.document.addEventListener("securitypolicyviolation", onViolation);
  try {
    result.attempts = 1;
    transport = new runtime.WebTransport(config.endpoint);
    // Observe closed immediately, including failure before ready, without logging error strings.
    closed = transport.closed.then(() => "closed", () => "rejected");
    result.outcome = await wait(transport.ready.then(() => "ready", () => "handshake-rejected"), config.readyMs, "ready-timeout");
  } catch {
    result.outcome = transport ? "evaluation-failed" : "constructor-rejected";
  } finally {
    if (transport) {
      try {
        transport.close();
        result.close = closed ? await wait(closed, config.closeMs, "close-timeout") : "context-forced";
      } catch { result.close = "close-threw"; }
    }
    runtime.document.removeEventListener("securitypolicyviolation", onViolation);
  }
  return result;
}

/** Disabled page scripts also disable page timers. Only the operator's timer callbacks run via CDP. */
export async function runCdpProbe(cdp, config) {
  const binding = "__wovenOriginProbeClock";
  const timers = new Map();
  let cspViolationObserved = false;
  const onBinding = (event) => {
    if (event.name !== binding || typeof event.payload !== "string" || event.payload.length > 128) return;
    let message;
    try { message = JSON.parse(event.payload); } catch { return; }
    if (!message || !Number.isInteger(message.id) || message.id < 1 || message.id > 2) return;
    if (message.operation === "clear") {
      clearTimeout(timers.get(message.id)); timers.delete(message.id);
    } else if (message.operation === "set" && !timers.has(message.id) && timers.size < 2 &&
               Number.isInteger(message.ms) && message.ms >= 1 && message.ms <= LIMITS.readyMs) {
      timers.set(message.id, setTimeout(() => {
        timers.delete(message.id);
        void cdp.send("Runtime.evaluate", {
          expression: `globalThis.__WOVEN_ORIGIN_PROBE_CLOCK__?.fire(${message.id}); void 0;`,
        }).catch(() => {});
      }, message.ms));
    }
  };
  const onIssue = ({ issue }) => {
    const details = issue.details?.contentSecurityPolicyIssueDetails;
    if (issue.code === "ContentSecurityPolicyIssue" && details?.isReportOnly === false &&
        details.blockedURL === config.endpoint && /^(?:connect-src|default-src)(?: |$)/u.test(details.violatedDirective)) {
      cspViolationObserved = true;
    }
  };
  cdp.on("Runtime.bindingCalled", onBinding);
  cdp.on("Audits.issueAdded", onIssue);
  try {
    await cdp.send("Runtime.enable");
    await cdp.send("Audits.enable");
    await cdp.send("Runtime.addBinding", { name: binding });
    const invoke = async (config, probe) => {
      let nextId = 0;
      const callbacks = new Map();
      globalThis.__WOVEN_ORIGIN_PROBE_CLOCK__ = {
        fire: (id) => { const callback = callbacks.get(id); callbacks.delete(id); callback?.(); },
      };
      try {
        return await probe(config, {
          origin: globalThis.origin, location, isSecureContext, WebTransport: globalThis.WebTransport, document,
          setTimeout: (callback, ms) => {
            const id = ++nextId; callbacks.set(id, callback);
            globalThis.__wovenOriginProbeClock(JSON.stringify({ operation: "set", id, ms }));
            return id;
          },
          clearTimeout: (id) => {
            callbacks.delete(id);
            globalThis.__wovenOriginProbeClock(JSON.stringify({ operation: "clear", id }));
          },
        });
      } finally { callbacks.clear(); delete globalThis.__WOVEN_ORIGIN_PROBE_CLOCK__; }
    };
    const evaluated = await cdp.send("Runtime.evaluate", {
      expression: `(${invoke.toString()})(${JSON.stringify(config)}, (${probeTransport.toString()}))`,
      awaitPromise: true, returnByValue: true,
    });
    if (evaluated.exceptionDetails || !evaluated.result?.value) throw new Error("probe-evaluation-failed");
    return { ...evaluated.result.value, cspViolationObserved: cspViolationObserved || evaluated.result.value.cspViolationObserved === true };
  } finally {
    for (const timer of timers.values()) clearTimeout(timer);
    cdp.off("Runtime.bindingCalled", onBinding);
    cdp.off("Audits.issueAdded", onIssue);
  }
}

const OUTCOMES = new Set([
  "ready", "handshake-rejected", "ready-timeout", "constructor-rejected", "document-origin-mismatch",
  "insecure-context", "webtransport-unavailable", "invalid-config", "document-failed", "evaluation-failed",
]);
const CLOSES = new Set(["not-opened", "closed", "rejected", "close-timeout", "close-threw", "context-forced"]);

export function summarizeResults(results) {
  if (!Array.isArray(results) || results.length > ORIGIN_CASES.length) throw new Error("invalid-results");
  const cases = results.map((result, index) => {
    const expected = ORIGIN_CASES[index];
    if (result.origin !== expected.origin || !OUTCOMES.has(result.outcome) || !CLOSES.has(result.close) ||
        ![0, 1, null].includes(result.attempts)) throw new Error("invalid-results");
    const observed = result.cspViolationObserved === false && (expected.expected === "ready" ?
      result.outcome === "ready" : result.outcome === "handshake-rejected");
    const cleanupObserved = result.close === "closed" || (result.outcome === "handshake-rejected" && result.close === "rejected");
    return {
      origin: expected.origin, expected: expected.expected, outcome: result.outcome,
      attempts: result.attempts, close: result.close, cspViolationObserved: result.cspViolationObserved === true,
      expectationMet: observed && result.attempts === 1 && cleanupObserved,
    };
  });
  return {
    ok: cases.length === ORIGIN_CASES.length && cases.every((result) => result.expectationMet),
    complete: cases.length === ORIGIN_CASES.length, cases,
    originPolicyVerified: false, wovenAuthentication: "not-attempted", wovenAdmission: "not-attempted", applicationMessagesSent: 0,
    interpretation: "Ready is transport-handshake readiness only, not WVN1 authentication or admission. A negative handshake rejection is unattributed: it cannot alone distinguish origin policy from TLS/network, HTTP authentication, or other endpoint failures. Timeouts and document/CSP failures are inconclusive, not origin-policy proof.",
  };
}

/** Both fixed loopback ports must be exclusively ours before any remote navigation. */
export async function startLocalDocuments(servers, endpoint, marker, serverFactory = createServer, signal) {
  for (const port of [8000, 8001]) {
    signal?.throwIfAborted();
    const server = serverFactory({
      requestTimeout: 1_000, headersTimeout: 1_000, keepAliveTimeout: 1,
      maxHeaderSize: 4_096, connectionsCheckingInterval: 1_000,
    }, (request, response) => {
      if (request.method !== "GET" || request.url !== "/" ||
          ![`127.0.0.1:${port}`, `localhost:${port}`].includes(request.headers.host)) {
        response.writeHead(404, { "Connection": "close" }).end(); return;
      }
      response.writeHead(200, {
        "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store", "Connection": "close",
        "X-Woven-Origin-Probe": marker,
        "Content-Security-Policy": `default-src 'none'; connect-src ${new URL(endpoint).origin}; base-uri 'none'; form-action 'none'; frame-ancestors 'none'`,
      });
      response.end("<!doctype html><title>Woven origin readiness probe</title>");
    });
    server.maxConnections = 8;
    server.maxRequestsPerSocket = 2;
    servers.push(server);
    await new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen({ port, host: "127.0.0.1", exclusive: true, signal }, resolve);
    });
  }
}

export async function main(args = process.argv.slice(2)) {
  let options;
  try { options = parseOptions(args); } catch { console.error(HELP); return 1; }
  if (options.help) { console.log(HELP); return 0; }
  const plan = {
    mode: options.run ? "run" : "inspect", endpoint: options.endpoint, origins: ORIGIN_CASES, limits: LIMITS,
    maximumConnections: ORIGIN_CASES.length, credentials: "none", applicationMessagesSent: 0,
    approval: "--run asserts prior operator approval for the explicit endpoint and these document origins",
  };
  if (!options.run) { console.log(JSON.stringify(plan, null, 2)); return 0; }
  if (unsafeLoggingEnvironment(process.env, process.execArgv)) {
    console.error("Origin probe refuses debug/protocol/TLS-key logging or NODE_OPTIONS environments."); return 1;
  }
  const started = performance.now();
  const startedAt = new Date().toISOString();
  const servers = [];
  const localCancellation = new AbortController();
  const results = [];
  const documents = [];
  let browserServer;
  let browser;
  let stage = "local-listeners";
  let failed = false;
  let failure = null;
  let browserTeardown = "not-opened";
  const forceCleanup = () => {
    localCancellation.abort();
    for (const server of servers) { server.closeAllConnections(); server.close(); }
    // This is only the child launched by this invocation, never a port owner or other browser.
    browserServer?.process().kill("SIGKILL");
  };
  const terminate = (code, reason) => {
    console.error(JSON.stringify({ ok: false, stage, code: reason }));
    try { forceCleanup(); } finally { process.exit(code); }
  };
  const onInterrupt = () => terminate(130, "operator-interrupt");
  const onTerminate = () => terminate(143, "operator-terminate");
  process.once("SIGINT", onInterrupt);
  process.once("SIGTERM", onTerminate);
  const watchdog = setTimeout(() => terminate(1, "overall-deadline"), LIMITS.overallMs);
  const step = (promise, cap) => bounded(promise, Math.max(1, Math.min(cap, LIMITS.workMs - (performance.now() - started))));
  try {
    const marker = randomUUID(); // Local listener ownership only; never an authentication credential.
    await step(startLocalDocuments(servers, options.endpoint, marker, createServer, localCancellation.signal), 2_000);
    stage = "browser-launch";
    const { chromium } = await step(import("playwright"), 5_000);
    const env = {};
    for (const key of ["PATH", "HOME", "XDG_RUNTIME_DIR", "LANG"]) {
      if (process.env[key] !== undefined) env[key] = process.env[key];
    }
    browserServer = await chromium.launchServer({
      headless: true, timeout: 10_000, env,
      // Only localhost DNS is pinned to our IPv4 listener; Document origins are unchanged.
      args: ["--host-resolver-rules=MAP localhost 127.0.0.1", "--disable-background-networking", "--disable-preconnect", "--dns-prefetch-disable"],
    });
    browser = await step(chromium.connect(browserServer.wsEndpoint(), { timeout: 5_000 }), 5_000);
    for (const { origin } of ORIGIN_CASES) {
      if (performance.now() - started >= LIMITS.workMs) throw new Error("work-deadline");
      stage = "document";
      const context = await step(browser.newContext({
        ignoreHTTPSErrors: false, bypassCSP: false, serviceWorkers: "block", acceptDownloads: false,
      }), 2_000);
      let result = { origin, outcome: "document-failed", attempts: 0, close: "not-opened", cspViolationObserved: false };
      try {
        const page = await step(context.newPage(), 2_000);
        const loaded = await step(loadOriginDocument(page, origin, origin.startsWith("http:") ? marker : undefined), 8_000);
        documents.push(loaded.document);
        stage = "transport";
        result = { ...result, outcome: "evaluation-failed", attempts: null, close: "context-forced" };
        const probed = await step(runCdpProbe(loaded.cdp, {
          origin, endpoint: options.endpoint, readyMs: LIMITS.readyMs, closeMs: LIMITS.closeMs,
        }), 5_000);
        result = { ...probed, origin };
      } catch { /* Fixed outcomes only: never echo document content, URLs from errors, or browser error strings. */ }
      finally {
        try { await step(context.close(), 500); }
        catch { failed = true; failure = "context-cleanup-failed"; }
      }
      results.push(result);
      if (failed) break;
    }
  } catch (error) {
    failed = true;
    failure = stage === "local-listeners" && error?.code === "EADDRINUSE" ? "local-port-in-use" : `${stage}-failed`;
  } finally {
    stage = "cleanup";
    localCancellation.abort();
    try {
      await bounded((async () => {
        for (const server of servers) {
          server.closeAllConnections();
          await new Promise((resolve) => server.close(resolve));
        }
        await browser?.close();
        await browserServer?.close();
      })(), LIMITS.cleanupMs);
      browserTeardown = browserServer ? "closed" : "not-opened";
    } catch {
      failed = true;
      failure ??= "cleanup-failed";
      try { forceCleanup(); browserTeardown = "forced"; } catch { browserTeardown = "failed"; }
    } finally {
      // A forced browser kill can leave driver handles settling. Keep the hard
      // deadline armed without making a clean invocation wait for the timer.
      watchdog.unref();
      process.removeListener("SIGINT", onInterrupt);
      process.removeListener("SIGTERM", onTerminate);
    }
  }
  const summary = summarizeResults(results);
  const output = {
    ...plan, ...summary, ok: !failed && summary.ok, documents, browserTeardown, failure,
    startedAt, endedAt: new Date().toISOString(),
    overallElapsedMs: Math.round(performance.now() - started),
  };
  console.log(JSON.stringify(output, null, 2));
  return output.ok ? 0 : 1;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) process.exitCode = await main();
