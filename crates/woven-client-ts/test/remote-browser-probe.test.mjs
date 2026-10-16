import assert from "node:assert/strict";
import { createServer } from "node:http";
import test from "node:test";
import { build } from "esbuild";
import { chromium } from "playwright";
import { loadProbeDocument, parseOptions } from "../scripts/remote-browser-probe.mjs";

const endpoint = "https://api.woven.host:4434/webtransport";
const target = { version: 1, url: endpoint, namespaceId: "11", sessionId: "22", spaceId: "3", spaceEpoch: "1" };
async function probeBundle() {
  const bundle = await build({
    entryPoints: [new URL("../scripts/remote-browser-probe-page.ts", import.meta.url).pathname],
    bundle: true, platform: "browser", format: "iife", globalName: "__WOVEN_REMOTE_PROBE__", write: false, logLevel: "silent",
  });
  return bundle.outputFiles[0].text;
}

test("CLI defaults to metadata-only inspection and has no credential/file/unsafe-TLS flags", () => {
  const options = parseOptions([]);
  assert.equal(options.run, false); assert.equal(options.pageUrl, "https://jdharrison.com/");
  assert.equal(options.endpoint, endpoint);
  for (const args of [
    ["--token=not-accepted"], ["--token-file=/not-read"], ["--ignore-https-errors"], ["--origin=https://spoof.test"],
    ["--page=http://insecure.test/"], ["--page=https://user:pass@site.test/"], ["--page=https://site.test/?secret=x"],
    ["--run", "--inspect"], ["--namespace-id=1"], ["--operator-input"], ["--channel-id=0"],
  ]) assert.throws(() => parseOptions(args));
});

test("real local document is loaded, site scripts/resources cannot join; only sanitized metadata returns", { timeout: 20_000 }, async (t) => {
  let assetRequests = 0;
  const server = createServer((request, response) => {
    if (request.url !== "/") { assetRequests += 1; response.writeHead(404).end(); return; }
    response.writeHead(200, { "Content-Type": "text/html", "Content-Security-Policy": "connect-src 'none'" });
    response.end(`<!doctype html><title>Real local document</title>
      <meta name="weaver-lobby-bootstrap" content="/api/lobby">
      <meta name="weaver-lobby-target" content='${JSON.stringify(target)}'>
      <script>globalThis.siteScriptRan = true; fetch('/unwanted-bootstrap');</script>
      <script src="/unwanted-app.js"></script><iframe src="/weaver/index.html"></iframe>
            <body onload="globalThis.eventHandlerRan=true"><img src="/unwanted-image" onerror="globalThis.eventHandlerRan=true"></body>`);
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => { server.closeAllConnections(); await new Promise((resolve) => server.close(resolve)); });
  const origin = `http://127.0.0.1:${server.address().port}`;
  const browser = await chromium.launch({ headless: true, timeout: 5_000 });
  t.after(() => browser.close());
  const context = await browser.newContext({ serviceWorkers: "block" });
  const page = await context.newPage();
  const documentInfo = await loadProbeDocument(page, `${origin}/`, false, { browserJavaScript: await probeBundle(), endpoint });
  assert.equal(documentInfo.origin, origin); assert.equal(documentInfo.documentHttpStatus, 200);
  assert.equal(documentInfo.enforcedCspPresent, true);
  assert.deepEqual(documentInfo.documentScripts, ["/unwanted-app.js"]);
  assert.deepEqual(documentInfo.metadata.framePaths, ["/weaver/index.html"]);
  const evidence = await page.evaluate(({ origin, endpoint }) => ({
    title: document.title, origin: location.origin, url: document.URL, siteScriptRan: globalThis.siteScriptRan === true,
    eventHandlerRan: globalThis.eventHandlerRan === true, eventAttributes: document.querySelectorAll("[onload],[onerror]").length,
    originalNodes: document.querySelectorAll("script,iframe,img,object,embed").length,
    metadata: globalThis.__WOVEN_REMOTE_PROBE__.inspectDocument(document, origin, endpoint),
  }), { origin, endpoint });
  assert.equal(evidence.title, "Woven remote browser probe"); assert.equal(evidence.origin, origin);
  assert.equal(evidence.url, `${origin}/`); assert.equal(evidence.originalNodes, 0);
  assert.equal(evidence.siteScriptRan, false); assert.equal(evidence.eventHandlerRan, false);
  assert.equal(evidence.eventAttributes, 0); assert.equal(assetRequests, 0);
  assert.deepEqual(evidence.metadata.target, target); assert.equal(evidence.metadata.bootstrapValid, true);
  assert.deepEqual(evidence.metadata.framePaths, []);
  const invalid = await page.evaluate(({ origin, endpoint }) => {
    document.querySelector('meta[name="weaver-lobby-target"]').content = JSON.stringify({ token: "synthetic-not-output" });
    return globalThis.__WOVEN_REMOTE_PROBE__.inspectDocument(document, origin, endpoint);
  }, { origin, endpoint });
  assert.equal(invalid.targetValid, false); assert.equal(JSON.stringify(invalid).includes("synthetic-not-output"), false);

  // Exercise trusted input cancellation, not a remote connection or credential fetch.
  const pending = page.evaluate(({ origin, endpoint, target }) =>
    globalThis.__WOVEN_REMOTE_PROBE__.runRemoteProbe({ origin, endpoint, target, channelId: "1", operatorInput: true, timeoutMs: 3_000 }),
  { origin, endpoint, target });
  await page.waitForSelector('dialog input[type="password"]', { timeout: 2_000 });
  await page.keyboard.press("Escape");
  const result = await pending;
  assert.equal(result.ok, false); assert.equal(result.code, "operator-cancelled");
  assert.equal(result.close, "not-opened"); assert.deepEqual(result.outboundKinds, []);
  assert.equal(await page.locator("dialog").count(), 0);
});

test("closed declarative shadow handlers are discarded before bootstrap/input; original URL and header/meta CSP stay active", { timeout: 20_000 }, async (t) => {
  const requests = [];
  const server = createServer((request, response) => {
    requests.push(request.url);
    if (request.url === "/api/lobby") {
      response.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store, private" });
      response.end(JSON.stringify({ ...target, token: "a".repeat(64) }));
      return;
    }
    if (request.url !== "/") { response.writeHead(404).end(); return; }
    response.writeHead(200, {
      "Content-Type": "text/html",
      "Content-Security-Policy": `connect-src http://${request.headers.host}/api/lobby`,
    });
    response.end(`<!doctype html><head>
      <meta http-equiv="Content-Security-Policy" content="img-src 'none'">
      <meta name="weaver-lobby-bootstrap" content="/api/lobby">
      <meta name="weaver-lobby-target" content='${JSON.stringify(target)}'>
      </head><body><div id="shadow-host"><template shadowrootmode="closed">
        <input onfocus="globalThis.shadowHandlerRan=true">
      </template></div></body>`);
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(async () => { server.closeAllConnections(); await new Promise((resolve) => server.close(resolve)); });
  const origin = `http://127.0.0.1:${server.address().port}`;
  const browser = await chromium.launch({ headless: true, timeout: 5_000 });
  t.after(() => browser.close());
  const context = await browser.newContext({ serviceWorkers: "block" });

  // Script-enabled positive control: the parser creates a closed root whose
  // real handler survives light-DOM sanitization. The probe itself loads disabled.
  const control = await context.newPage();
  const controlCdp = await context.newCDPSession(control);
  await control.goto(`${origin}/`, { waitUntil: "domcontentloaded", timeout: 5_000 });
  const { root } = await controlCdp.send("DOM.getDocument", { depth: -1, pierce: true });
  const { nodeId } = await controlCdp.send("DOM.querySelector", { nodeId: root.nodeId, selector: "#shadow-host" });
  const { node } = await controlCdp.send("DOM.describeNode", { nodeId, depth: -1, pierce: true });
  assert.equal(node.shadowRoots[0].shadowRootType, "closed");
  const shadowInput = node.shadowRoots[0].children.find((child) => child.nodeName === "INPUT");
  assert.ok(shadowInput);
  await controlCdp.send("Runtime.evaluate", {
    expression: 'document.querySelectorAll("*").forEach(node => node.getAttributeNames().filter(name => name.startsWith("on")).forEach(name => node.removeAttribute(name)))',
  });
  const { object } = await controlCdp.send("DOM.resolveNode", { backendNodeId: shadowInput.backendNodeId });
  const focused = await controlCdp.send("Runtime.callFunctionOn", {
    objectId: object.objectId,
    functionDeclaration: "function() { this.focus(); this.dispatchEvent(new Event('focus')); return { handler: typeof this.onfocus, ran: globalThis.shadowHandlerRan === true }; }",
    returnByValue: true,
  });
  assert.deepEqual(focused.result.value, { handler: "function", ran: true });
  await control.close();

  const page = await context.newPage();
  const documentInfo = await loadProbeDocument(page, `${origin}/`, true, { browserJavaScript: await probeBundle(), endpoint });
  assert.deepEqual(documentInfo.metadata.target, target); assert.equal(documentInfo.metadata.bootstrapValid, true);
  assert.equal(documentInfo.metadata.metaCspPresent, true);
  assert.equal(documentInfo.enforcedCspPresent, true);
  assert.deepEqual(await page.evaluate(() => ({
    url: document.URL, origin: location.origin, bodyChildren: document.body.children.length,
    shadowHosts: document.querySelectorAll("#shadow-host,template").length,
    handlerRan: globalThis.shadowHandlerRan === true,
  })), { url: `${origin}/`, origin, bodyChildren: 0, shadowHosts: 0, handlerRan: false });

  const bootstrap = await page.evaluate(async ({ origin }) => {
    globalThis.transportAttempts = 0;
    globalThis.WebTransport = class { constructor() { globalThis.transportAttempts += 1; throw new Error("mock transport only"); } };
    const target = JSON.parse(document.querySelector('meta[name="weaver-lobby-target"]').content);
    const path = document.querySelector('meta[name="weaver-lobby-bootstrap"]').content;
    const credential = await globalThis.__WOVEN_REMOTE_PROBE__.fetchBootstrap(path, target, origin, new AbortController().signal);
    return { credentialValid: credential.length === 64, handlerRan: globalThis.shadowHandlerRan === true };
  }, { origin });
  assert.deepEqual(bootstrap, { credentialValid: true, handlerRan: false });
  assert.equal(requests.filter((path) => path === "/api/lobby").length, 1);

  const pending = page.evaluate(({ origin, endpoint }) => globalThis.__WOVEN_REMOTE_PROBE__.runRemoteProbe({
    origin, endpoint, channelId: "1", operatorInput: true, timeoutMs: 3_000,
  }), { origin, endpoint });
  await page.waitForSelector('dialog input[type="password"]', { timeout: 2_000 });
  await page.locator('dialog input[type="password"]').focus();
  await page.keyboard.press("Escape");
  assert.equal((await pending).code, "operator-cancelled");
  assert.deepEqual(await page.evaluate(() => ({
    handlerRan: globalThis.shadowHandlerRan === true, transportAttempts: globalThis.transportAttempts,
  })), { handlerRan: false, transportAttempts: 0 });

  // Removing the old DOM must not remove either response-header or meta CSP.
  await page.evaluate(() => {
    globalThis.cspDirectives = [];
    document.addEventListener("securitypolicyviolation", (event) => {
      if (globalThis.cspDirectives.length < 2) globalThis.cspDirectives.push(event.effectiveDirective);
    });
    void fetch("/blocked-fetch").catch(() => {});
    const image = document.createElement("img"); image.src = "/blocked-image"; document.body.append(image);
  });
  await page.waitForFunction(() => ["connect-src", "img-src"].every((directive) => globalThis.cspDirectives.includes(directive)), undefined, { timeout: 2_000 });
  assert.equal(requests.some((path) => path.startsWith("/blocked-")), false);
});
