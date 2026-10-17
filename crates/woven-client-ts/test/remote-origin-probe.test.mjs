import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";
import {
  LIMITS, ORIGIN_CASES, loadOriginDocument, main, parseOptions, probeTransport, runCdpProbe,
  startLocalDocuments, summarizeResults, unsafeLoggingEnvironment,
} from "../scripts/remote-origin-probe.mjs";

const endpoint = "https://api.woven.host:4434/webtransport";
const origin = ORIGIN_CASES[0].origin;
const config = { endpoint, origin, readyMs: 10, closeMs: 10 };
const pending = () => new Promise(() => {});

function mockRuntime({ ready = Promise.resolve(), closed = Promise.resolve(), constructThrows = false, closeThrows = false } = {}) {
  const calls = [];
  const timers = new Set();
  const listeners = new Map();
  const runtime = {
    origin, location: { origin }, isSecureContext: true,
    document: {
      addEventListener: (name, listener) => listeners.set(name, listener),
      removeEventListener: (name, listener) => { assert.equal(listeners.get(name), listener); listeners.delete(name); },
    },
    setTimeout: (fn, ms) => { const timer = setTimeout(fn, ms); timers.add(timer); return timer; },
    clearTimeout: (timer) => { clearTimeout(timer); timers.delete(timer); },
    WebTransport: class {
      constructor(...args) {
        calls.push(["construct", ...args]);
        if (constructThrows) throw new Error("private mock error must not be printed");
        this.ready = ready;
        this.closed = closed;
      }
      close(...args) { calls.push(["close", ...args]); if (closeThrows) throw new Error("private close error"); }
      createBidirectionalStream() { assert.fail("no streams"); }
      createUnidirectionalStream() { assert.fail("no streams"); }
      get datagrams() { assert.fail("no datagrams"); }
    },
  };
  return { runtime, calls, timers, listeners };
}

function clean(mock) {
  assert.equal(mock.timers.size, 0);
  assert.equal(mock.listeners.size, 0);
}

test("inspect is a no-network plan, with the exact origins and fixed bounds", async (t) => {
  assert.deepEqual(parseOptions([]), { help: false, run: false, endpoint: null });
  const output = [];
  t.mock.method(console, "log", (value) => output.push(JSON.parse(value)));
  assert.equal(await main([]), 0);
  assert.equal(await main(["--inspect", `--endpoint=${endpoint}`]), 0);
  assert.equal(output[0].mode, "inspect");
  assert.equal(output[0].endpoint, null);
  assert.equal(output[1].endpoint, endpoint);
  assert.deepEqual(output[0].origins, [
    { origin: "http://127.0.0.1:8000", expected: "ready" },
    { origin: "http://localhost:8000", expected: "ready" },
    { origin: "https://woven.host", expected: "ready" },
    { origin: "https://jdharrison.com", expected: "ready" },
    { origin: "http://127.0.0.1:8001", expected: "rejected" },
  ]);
  assert.equal(output[0].maximumConnections, 5);
  assert.equal(output[0].applicationMessagesSent, 0);
  assert.ok(LIMITS.workMs + LIMITS.cleanupMs < LIMITS.overallMs && LIMITS.overallMs < 90_000);
  assert.equal(LIMITS.readyMs, 4_000);
  assert.equal(LIMITS.closeMs, 500);
});

test("run requires an explicit canonical HTTPS endpoint and rejects unsafe/unrelated arguments", () => {
  assert.equal(parseOptions(["--run", `--endpoint=${endpoint}`]).run, true);
  assert.equal(parseOptions(["--help"]).help, true);
  for (const args of [
    ["--run"], ["--run", "--inspect", `--endpoint=${endpoint}`], ["--inspect", "--inspect"],
    [`--endpoint=${endpoint}`, `--endpoint=${endpoint}`], ["--run=true"], ["--endpoint"],
    ["--token=private"], ["--token-file=/private"], ["--operator-input"], ["--bootstrap-path=/api/lobby"],
    ["--namespace-id=1"], ["--session-id=1"], ["--origin=https://spoof.invalid"], ["--page=https://elsewhere.invalid"],
    ["--ignore-https-errors"], ["--server-certificate-hash=x"], ["--timeout=120000"], ["--workers=2"],
    ["--inspect", "--help", `--endpoint=${endpoint}`, "--unknown"],
  ]) assert.throws(() => parseOptions(args));
  for (const value of [
    "", "http://api.woven.host:4434/webtransport", "quic://api.woven.host:4433", "https://api.woven.host/",
    "https://user:password@api.woven.host/webtransport", `${endpoint}?token=private`, `${endpoint}#private`,
    `${endpoint}?`, `${endpoint}#`, "https://API.woven.host:4434/webtransport", "https://api.woven.host:443/webtransport",
    "https://api.woven.host/a/../webtransport", "https://api.woven.host/%77ebtransport", `${endpoint} `,
    "https://api.woven.host\\webtransport", `https://${"a".repeat(512)}.invalid/webtransport`,
  ]) assert.throws(() => parseOptions([`--endpoint=${value}`]));
});

test("debug, inspector, and TLS-key logging environments are refused", () => {
  assert.equal(unsafeLoggingEnvironment({}, []), false);
  for (const key of ["DEBUG", "PWDEBUG", "SSLKEYLOGFILE", "NODE_DEBUG", "NODE_DEBUG_NATIVE", "NODE_OPTIONS", "CHROME_LOG_FILE"]) {
    assert.equal(unsafeLoggingEnvironment({ [key]: "unsafe" }), true);
  }
  for (const flag of ["--inspect", "--inspect-brk=9229", "--tls-keylog=/private", "--debug"]) {
    assert.equal(unsafeLoggingEnvironment({}, [flag]), true);
  }
});

test("one native WebTransport, ready then close, no options, streams, or application traffic", async () => {
  const mock = mockRuntime();
  const result = await probeTransport(config, mock.runtime);
  assert.deepEqual(result, { outcome: "ready", attempts: 1, close: "closed", cspViolationObserved: false });
  assert.deepEqual(mock.calls, [["construct", endpoint], ["close"]]);
  clean(mock);
});

test("handshake rejection is generic, sanitized, observed closed, and never retried", async () => {
  const mock = mockRuntime({ ready: Promise.reject(new Error("private authentication/origin error")), closed: Promise.reject(new Error("private close")) });
  const result = await probeTransport(config, mock.runtime);
  assert.equal(result.outcome, "handshake-rejected");
  assert.equal(result.close, "rejected");
  assert.equal(result.attempts, 1);
  assert.deepEqual(mock.calls, [["construct", endpoint], ["close"]]);
  assert.equal(JSON.stringify(result).includes("private"), false);
  clean(mock);
});

test("readiness and closure have separate bounded timeouts, with exactly one close", async () => {
  const mock = mockRuntime({ ready: pending(), closed: pending() });
  const result = await probeTransport({ ...config, readyMs: 1, closeMs: 1 }, mock.runtime);
  assert.equal(result.outcome, "ready-timeout");
  assert.equal(result.close, "close-timeout");
  assert.equal(result.attempts, 1);
  assert.deepEqual(mock.calls, [["construct", endpoint], ["close"]]);
  clean(mock);
});

test("constructor and close failures do not expose errors or retry", async () => {
  for (const options of [{ constructThrows: true }, { closeThrows: true }]) {
    const mock = mockRuntime(options);
    const result = await probeTransport(config, mock.runtime);
    assert.equal(result.outcome, options.constructThrows ? "constructor-rejected" : "ready");
    assert.equal(result.close, options.constructThrows ? "not-opened" : "close-threw");
    assert.equal(result.attempts, 1);
    assert.equal(mock.calls.length, options.constructThrows ? 1 : 2);
    assert.equal(JSON.stringify(result).includes("private"), false);
    clean(mock);
  }
});

test("origin/context/API preconditions and invalid budgets fail without constructing a transport", async () => {
  for (const [patch, expected] of [
    [{ origin: "null" }, "document-origin-mismatch"],
    [{ location: { origin: "http://localhost:8000" } }, "document-origin-mismatch"],
    [{ isSecureContext: false }, "insecure-context"], [{ WebTransport: undefined }, "webtransport-unavailable"],
  ]) {
    const mock = mockRuntime(); Object.assign(mock.runtime, patch);
    assert.equal((await probeTransport(config, mock.runtime)).outcome, expected);
    assert.deepEqual(mock.calls, []); clean(mock);
  }
  for (const patch of [
    { readyMs: 0 }, { readyMs: 4_001 }, { readyMs: 1.5 }, { readyMs: Infinity }, { closeMs: 0 }, { closeMs: 501 },
    { endpoint: "http://unsafe.invalid/webtransport" }, { endpoint: `${endpoint}?token=private` },
    { endpoint: "https://user:password@unsafe.invalid/webtransport" },
  ]) {
    const mock = mockRuntime();
    assert.equal((await probeTransport({ ...config, ...patch }, mock.runtime)).outcome, "invalid-config");
    assert.deepEqual(mock.calls, []); clean(mock);
  }
});

test("an observed enforcing CSP failure is not labeled a successful negative origin-policy check", async () => {
  const mock = mockRuntime({ ready: pending() });
  const running = probeTransport({ ...config, readyMs: 1 }, mock.runtime);
  const listener = mock.listeners.get("securitypolicyviolation");
  listener({ disposition: "report", effectiveDirective: "connect-src" });
  listener({ disposition: "enforce", effectiveDirective: "img-src" });
  listener({ disposition: "enforce", effectiveDirective: "connect-src", blockedURI: "private-not-output" });
  assert.equal((await running).cspViolationObserved, true);
  clean(mock);
});

function mockDocument({ contentType = "text/html", marker = "owned", evidence = {}, ok = true, finalUrl = "https://woven.host/" } = {}) {
  const calls = [];
  const mainFrame = {};
  let routeHandler;
  const cdp = {
    send: async (method, params) => {
      calls.push([method, params]);
      if (method === "Runtime.evaluate") return { result: { value: {
        origin: "https://woven.host", locationOrigin: "https://woven.host", secureContext: true, webTransportAvailable: true, ...evidence,
      } } };
      return {};
    },
  };
  const context = {
    newCDPSession: async () => cdp,
    route: async (pattern, handler) => { assert.equal(pattern, "**/*"); routeHandler = handler; },
  };
  const request = async ({ url = "https://woven.host/", frame = mainFrame, navigation = true, method = "GET" } = {}) => {
    let action;
    await routeHandler({
      request: () => ({ url: () => url, frame: () => frame, isNavigationRequest: () => navigation, method: () => method }),
      continue: async (...args) => { assert.deepEqual(args, []); action = "continued"; },
      abort: async (reason) => { assert.equal(reason, "blockedbyclient"); action = "aborted"; },
      fulfill: () => assert.fail("must never fulfill a main document"),
    });
    return action;
  };
  const page = {
    context: () => context, mainFrame: () => mainFrame, url: () => finalUrl,
    goto: async (url, options) => {
      calls.push(["goto", url, options]);
      assert.equal(await request(), "continued");
      for (const patch of [
        { navigation: false }, { frame: {} }, { method: "POST" }, { url: "https://elsewhere.invalid/" },
        { url: "https://woven.host/?secret=private" }, { url: "https://user:password@woven.host/" },
        { url: "https://woven.host/api/lobby" }, { url: "https://woven.host/login" },
      ]) assert.equal(await request(patch), "aborted");
      for (let i = 1; i < LIMITS.documentRequests; i += 1) assert.equal(await request(), "continued");
      assert.equal(await request(), "aborted");
      return { ok: () => ok, status: () => ok ? 200 : 403, headers: () => ({ "content-type": contentType, "x-woven-origin-probe": marker }) };
    },
  };
  return { page, calls, request };
}

test("real main-document requests only, resources/redirect escapes blocked, scripts never re-enabled", async () => {
  const mock = mockDocument();
  const result = await loadOriginDocument(mock.page, "https://woven.host", "owned");
  assert.equal(result.document.origin, "https://woven.host");
  assert.equal(result.document.httpStatus, 200);
  assert.deepEqual(mock.calls[0], ["Emulation.setScriptExecutionDisabled", { value: true }]);
  assert.equal(mock.calls.filter(([method]) => method === "Emulation.setScriptExecutionDisabled").length, 1);
  assert.equal(mock.calls.find(([method]) => method === "goto")[2].timeout, LIMITS.navigationMs);
  assert.equal(mock.calls.some(([method]) => method === "Page.stopLoading"), true);
  assert.equal(await mock.request(), "aborted");
});

test("document errors, wrong actual/opaque origins, cross-origin redirects, or wrong listener ownership fail closed", async () => {
  for (const patch of [
    { ok: false }, { contentType: "application/json" }, { marker: "another-process" },
    { evidence: { origin: "null" } }, { evidence: { locationOrigin: "https://elsewhere.invalid" } },
    { finalUrl: "https://elsewhere.invalid/" }, { finalUrl: "https://woven.host/?private" }, { finalUrl: "https://woven.host/login" },
  ]) {
    const mock = mockDocument(patch);
    await assert.rejects(loadOriginDocument(mock.page, "https://woven.host", "owned"));
  }
});

test("CDP clock/audit plumbing stays bounded, sanitizes CSP evidence, and removes its listeners", async () => {
  const cdp = new EventEmitter();
  const methods = [];
  cdp.send = async (method, params) => {
    methods.push(method);
    if (method !== "Runtime.evaluate") return {};
    assert.equal(params.awaitPromise, true);
    for (const payload of ["not-json", "null", JSON.stringify({ operation: "set", id: 3, ms: 1 }), JSON.stringify({ operation: "set", id: 1, ms: 90_000 })]) {
      cdp.emit("Runtime.bindingCalled", { name: "__wovenOriginProbeClock", payload });
    }
    cdp.emit("Runtime.bindingCalled", { name: "__wovenOriginProbeClock", payload: JSON.stringify({ operation: "set", id: 1, ms: 10 }) });
    cdp.emit("Runtime.bindingCalled", { name: "__wovenOriginProbeClock", payload: JSON.stringify({ operation: "clear", id: 1 }) });
    cdp.emit("Audits.issueAdded", { issue: {
      code: "ContentSecurityPolicyIssue", details: { contentSecurityPolicyIssueDetails: {
        isReportOnly: false, blockedURL: endpoint, violatedDirective: "connect-src 'none'", privateField: "never-output",
      } },
    } });
    return { result: { value: { outcome: "handshake-rejected", attempts: 1, close: "rejected", cspViolationObserved: false } } };
  };
  const result = await runCdpProbe(cdp, config);
  assert.equal(result.cspViolationObserved, true);
  assert.equal(JSON.stringify(result).includes("never-output"), false);
  assert.deepEqual(methods, ["Runtime.enable", "Audits.enable", "Runtime.addBinding", "Runtime.evaluate"]);
  assert.deepEqual(cdp.eventNames(), []);
});

function expectedResults() {
  return ORIGIN_CASES.map(({ origin, expected }) => ({
    origin, outcome: expected === "ready" ? "ready" : "handshake-rejected", attempts: 1,
    close: expected === "ready" ? "closed" : "rejected", cspViolationObserved: false,
  }));
}

test("results distinguish readiness from authentication/admission and never prove rejection attribution", () => {
  const results = expectedResults();
  const summary = summarizeResults(results);
  assert.equal(summary.ok, true);
  assert.equal(summary.complete, true);
  assert.equal(summary.originPolicyVerified, false);
  assert.equal(summary.wovenAuthentication, "not-attempted");
  assert.equal(summary.wovenAdmission, "not-attempted");
  assert.equal(summary.applicationMessagesSent, 0);
  assert.match(summary.interpretation, /unattributed/u);
  assert.match(summary.interpretation, /HTTP authentication/u);
  assert.equal(summarizeResults([]).ok, false);
  assert.equal(summarizeResults(results.slice(0, 4)).complete, false);
  for (const patch of [
    { outcome: "ready" }, { outcome: "ready-timeout" }, { outcome: "document-failed", attempts: 0 },
    { outcome: "constructor-rejected" }, { cspViolationObserved: true }, { close: "close-timeout" }, { attempts: null },
  ]) {
    const modified = expectedResults(); Object.assign(modified[4], patch);
    assert.equal(summarizeResults(modified).ok, false);
  }
  for (const close of ["close-timeout", "close-threw", "context-forced", "rejected"]) {
    const modified = expectedResults(); modified[0].close = close;
    assert.equal(summarizeResults(modified).ok, false);
  }
  results[0].rawError = "private must not be reflected";
  assert.equal(JSON.stringify(summarizeResults(results)).includes("private"), false);
});

test("result boundary rejects duplicate/reordered/arbitrary origins, retries, and invented success outcomes", () => {
  for (const patch of [
    { origin: "http://localhost:9999" }, { origin: ORIGIN_CASES[1].origin }, { attempts: 2 },
    { outcome: "authenticated" }, { outcome: "admitted" }, { close: "probably-closed" },
  ]) {
    const results = expectedResults(); Object.assign(results[0], patch);
    assert.throws(() => summarizeResults(results), /invalid-results/u);
  }
  assert.throws(() => summarizeResults([...expectedResults(), expectedResults()[0]]), /invalid-results/u);
});

test("listener factory binds only owned loopback ports, caps resources, and fails on occupied ports without killing anything", async () => {
  const owned = [];
  const handlers = [];
  const listenOptions = [];
  const factory = (options, handler) => {
    assert.equal(options.requestTimeout, 1_000);
    assert.equal(options.maxHeaderSize, 4_096);
    handlers.push(handler);
    const server = new EventEmitter();
    server.listen = (options, callback) => { listenOptions.push(options); queueMicrotask(callback); };
    return server;
  };
  await startLocalDocuments(owned, endpoint, "owned", factory);
  assert.deepEqual(listenOptions.map(({ port, host, exclusive }) => ({ port, host, exclusive })), [
    { port: 8000, host: "127.0.0.1", exclusive: true }, { port: 8001, host: "127.0.0.1", exclusive: true },
  ]);
  assert.ok(owned.every((server) => server.maxConnections === 8 && server.maxRequestsPerSocket === 2));
  for (const [host, path, method, status] of [
    ["127.0.0.1:8000", "/", "GET", 200], ["localhost:8000", "/", "GET", 200],
    ["elsewhere.invalid", "/", "GET", 404], ["localhost:8000", "/api/lobby", "GET", 404], ["localhost:8000", "/", "POST", 404],
  ]) {
    const response = { writeHead: (code, headers) => {
      assert.equal(code, status);
      if (status === 200) {
        assert.equal(headers["X-Woven-Origin-Probe"], "owned");
        assert.equal(headers["Cache-Control"], "no-store");
        assert.match(headers["Content-Security-Policy"], /default-src 'none'/u);
      }
      return response;
    }, end: (body) => { if (status === 200) assert.equal(body.includes("<script"), false); } };
    handlers[0]({ method, url: path, headers: { host } }, response);
  }
  for (const busyPort of [8000, 8001]) {
    const failedOwned = [];
    await assert.rejects(startLocalDocuments(failedOwned, endpoint, "owned", (...args) => {
      const server = factory(...args);
      server.listen = ({ port }, callback) => queueMicrotask(() => {
        if (port === busyPort) server.emit("error", Object.assign(new Error("busy"), { code: "EADDRINUSE" }));
        else callback();
      });
      return server;
    }), /busy/u);
    assert.equal(failedOwned.length, busyPort === 8000 ? 1 : 2);
  }
  const cancellation = new AbortController(); cancellation.abort();
  await assert.rejects(startLocalDocuments([], endpoint, "owned", () => assert.fail("no late listeners"), cancellation.signal));
});
