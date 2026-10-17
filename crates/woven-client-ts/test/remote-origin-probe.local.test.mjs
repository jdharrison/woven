import assert from "node:assert/strict";
import { createServer } from "node:http";
import test from "node:test";
import { chromium } from "playwright";
import { loadOriginDocument, runCdpProbe } from "../scripts/remote-origin-probe.mjs";

// Separate from the pure mock suite: real loopback HTML/Chromium, but no real WebTransport or remote navigation.
test("CDP probes a real Document while site scripts/handlers remain disabled and resources stay blocked", { timeout: 20_000 }, async (t) => {
  let documents = 0;
  let assets = 0;
  const server = createServer((request, response) => {
    if (request.url !== "/") { assets += 1; response.writeHead(404).end(); return; }
    documents += 1;
    response.writeHead(200, {
      "Content-Type": "text/html", "X-Woven-Origin-Probe": "local-test",
      "Content-Security-Policy": "connect-src 'none'",
    });
    response.end(`<!doctype html><title>Original local Document</title>
      <script>globalThis.siteRan=true;fetch('/unwanted-bootstrap');new WebTransport('https://not-contacted.invalid/webtransport');</script>
      <script src="/unwanted.js"></script><iframe src="/unwanted-frame"></iframe><img src="/unwanted-image">
      <body onload="globalThis.handlerRan=true"><button onclick="globalThis.handlerRan=true">Test</button></body>`);
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => { server.closeAllConnections(); await new Promise((resolve) => server.close(resolve)); });
  const origin = `http://127.0.0.1:${server.address().port}`;
  const env = {};
  for (const key of ["PATH", "HOME", "XDG_RUNTIME_DIR", "LANG"]) {
    if (process.env[key] !== undefined) env[key] = process.env[key];
  }
  const browser = await chromium.launch({ headless: true, timeout: 5_000, env });
  t.after(() => browser.close());
  const context = await browser.newContext({ serviceWorkers: "block", bypassCSP: false, ignoreHTTPSErrors: false });
  const page = await context.newPage();
  const { cdp, document } = await loadOriginDocument(page, origin, "local-test");
  assert.equal(document.origin, origin);
  assert.equal(document.enforcedCspPresent, true);
  assert.equal(document.secureContext, true);
  const installed = await cdp.send("Runtime.evaluate", {
    expression: `globalThis.operatorCalls=[];globalThis.WebTransport=class {
      constructor(url) { operatorCalls.push('construct');this.ready=Promise.resolve();this.closed=Promise.resolve(); }
      close() { operatorCalls.push('close'); }
    };void 0;`,
  });
  assert.equal(installed.exceptionDetails, undefined);
  const probed = await runCdpProbe(cdp, {
    origin, endpoint: "https://not-contacted.invalid/webtransport", readyMs: 100, closeMs: 100,
  });
  assert.deepEqual(probed, { outcome: "ready", attempts: 1, close: "closed", cspViolationObserved: false });
  const evidence = await cdp.send("Runtime.evaluate", {
    expression: `document.querySelector('button').click();({
      origin: globalThis.origin, url: document.URL, title: document.title,
      siteRan: globalThis.siteRan===true, handlerRan: globalThis.handlerRan===true,
      scriptNodes: document.querySelectorAll('script').length, operatorCalls: globalThis.operatorCalls
    })`, returnByValue: true,
  });
  assert.deepEqual(evidence.result.value, {
    origin, url: `${origin}/`, title: "Original local Document", siteRan: false, handlerRan: false,
    scriptNodes: 2, operatorCalls: ["construct", "close"],
  });
  await cdp.send("Runtime.evaluate", {
    expression: `globalThis.WebTransport=class {
      constructor() { this.ready=new Promise(()=>{});this.closed=new Promise(()=>{}); }
      close() {}
    };void 0;`,
  });
  let timer;
  try {
    const timed = await Promise.race([
      runCdpProbe(cdp, { origin, endpoint: "https://not-contacted.invalid/webtransport", readyMs: 10, closeMs: 10 }),
      new Promise((_resolve, reject) => { timer = setTimeout(() => reject(new Error("disabled-script-timers-stalled")), 1_000); }),
    ]);
    assert.equal(timed.outcome, "ready-timeout");
    assert.equal(timed.close, "close-timeout");
  } finally { clearTimeout(timer); }
  assert.equal(documents, 1);
  assert.equal(assets, 0);
});
