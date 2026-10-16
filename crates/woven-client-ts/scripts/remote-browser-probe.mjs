import { fileURLToPath } from "node:url";
import { build } from "esbuild";
import { chromium } from "playwright";

const HELP = `Usage: node scripts/remote-browser-probe.mjs [--inspect | --run] [--operator-input]
  [--page=https://jdharrison.com/] [--endpoint=https://api.woven.host:4434/webtransport]
  [--namespace-id=N --session-id=N --space-id=N --space-epoch=N] [--channel-id=1]
  [--bootstrap-path=/same-origin-route]
Default: inspect public HTML metadata only; no bootstrap fetch or Woven connection.
--run requires prior approval for this target/scope. One attempt; queued is not success.
--operator-input opens a trusted browser password dialog; never pass a token on the CLI.
Maximum 90 seconds including setup/input/teardown. No publishes, client logs, or saved artifacts.`;

function invalid() { throw new Error("invalid-options"); }
function canonicalHttps(value) {
  if (value.length > 512 || /[\s\\?#]/u.test(value)) invalid();
  const url = new URL(value);
  if (!value.startsWith("https://") || url.username || url.password) invalid();
  return url.href;
}
function id(value) {
  if (!/^[1-9][0-9]{0,19}$/.test(value) || BigInt(value) > 18_446_744_073_709_551_615n) invalid();
  return value;
}
export function parseOptions(args) {
  if (args.length > 12) invalid();
  const values = new Map();
  const switches = new Set(["--inspect", "--run", "--operator-input", "--help"]);
  const fields = new Set(["--page", "--endpoint", "--namespace-id", "--session-id", "--space-id", "--space-epoch", "--channel-id", "--bootstrap-path"]);
  for (const argument of args) {
    const separator = argument.indexOf("=");
    const key = separator < 0 ? argument : argument.slice(0, separator);
    if (values.has(key)) invalid();
    if (switches.has(key) && separator < 0) values.set(key, true);
    else if (fields.has(key) && separator >= 0 && argument.length <= 1_024) values.set(key, argument.slice(separator + 1));
    else invalid();
  }
  if (values.has("--help")) return { help: true };
  if (values.has("--inspect") && values.has("--run")) invalid();
  if (values.has("--operator-input") && !values.has("--run")) invalid();
  const pageUrl = canonicalHttps(values.get("--page") ?? "https://jdharrison.com/");
  const endpoint = canonicalHttps(values.get("--endpoint") ?? "https://api.woven.host:4434/webtransport");
  if (new URL(endpoint).pathname === "/") invalid();
  const scopeKeys = ["--namespace-id", "--session-id", "--space-id", "--space-epoch"];
  const suppliedScope = scopeKeys.filter((key) => values.has(key));
  if (suppliedScope.length !== 0 && suppliedScope.length !== scopeKeys.length) invalid();
  const target = suppliedScope.length === 0 ? undefined : {
    version: 1, url: endpoint,
    namespaceId: id(values.get("--namespace-id")), sessionId: id(values.get("--session-id")),
    spaceId: id(values.get("--space-id")), spaceEpoch: id(values.get("--space-epoch")),
  };
  const bootstrapPath = values.get("--bootstrap-path");
  if (bootstrapPath !== undefined) {
    if (!bootstrapPath.startsWith("/") || bootstrapPath.startsWith("//") || /[\s\\?#]/u.test(bootstrapPath) ||
        new URL(bootstrapPath, pageUrl).pathname !== bootstrapPath) invalid();
  }
  return {
    help: false, run: values.has("--run"), pageUrl,
    origin: new URL(pageUrl).origin, endpoint, target, bootstrapPath,
    channelId: id(values.get("--channel-id") ?? "1"), operatorInput: values.has("--operator-input"),
  };
}

/** Continue the real main-document request; never fulfill/replace it or set an Origin header. */
export async function loadProbeDocument(page, pageUrl, allowBootstrap, { browserJavaScript, endpoint }) {
  const origin = new URL(pageUrl).origin;
  const cdp = await page.context().newCDPSession(page);
  await cdp.send("Emulation.setScriptExecutionDisabled", { value: true });
  let navigating = true;
  let bootstrapRequests = 0;
  await page.route("**/*", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const sameOrigin = url.origin === origin && !url.username && !url.password && !url.search && !url.hash;
    if (navigating && request.isNavigationRequest() && request.frame() === page.mainFrame() && sameOrigin) {
      await route.continue();
    } else if (!navigating && allowBootstrap && request.resourceType() === "fetch" && sameOrigin &&
               request.method() === "GET" && bootstrapRequests++ === 0) {
      await route.continue();
    } else {
      await route.abort("blockedbyclient");
    }
  });
  const response = await page.goto(pageUrl, { waitUntil: "domcontentloaded", timeout: 15_000 });
  navigating = false;
  await cdp.send("Page.stopLoading");
  if (!response || !response.ok() || !response.headers()["content-type"]?.toLowerCase().startsWith("text/html") ||
      new URL(page.url()).origin !== origin || new URL(page.url()).search || new URL(page.url()).hash) {
    throw new Error("document-load-failed");
  }
  const installed = await cdp.send("Runtime.evaluate", {
    expression: `${browserJavaScript}\nglobalThis.__WOVEN_REMOTE_PROBE__ = __WOVEN_REMOTE_PROBE__; void 0;`,
  });
  if (installed.exceptionDetails) throw new Error("document-isolation-failed");
  // Extract only validated configuration while scripts are disabled, then discard
  // the entire old tree, including inaccessible closed declarative shadow roots.
  const prepare = (endpoint) => {
    window.stop();
    const paths = [];
    for (const script of document.querySelectorAll("script[src]")) {
      if (paths.length >= 8) break;
      try {
        const url = new URL(script.src);
        if (url.origin === location.origin && !url.username && !url.password && !url.search && !url.hash &&
            /^\/[a-zA-Z0-9_./-]{0,200}\.js$/.test(url.pathname) && !/[0-9a-f]{32}/i.test(url.pathname)) paths.push(url.pathname);
      } catch { /* Only validated public asset paths leave the browser. */ }
    }
    const metadata = globalThis.__WOVEN_REMOTE_PROBE__.inspectDocument(document, location.origin, endpoint);
    const bootstrapPath = metadata.bootstrapValid ? document.querySelector('meta[name="weaver-lobby-bootstrap"]').content : null;
    const html = document.createElement("html");
    const head = document.createElement("head");
    const title = document.createElement("title");
    title.textContent = "Woven remote browser probe";
    head.append(title);
    for (const [name, content] of [
      ["weaver-lobby-bootstrap", bootstrapPath],
      ["weaver-lobby-target", metadata.target === null ? null : JSON.stringify(metadata.target)],
    ]) {
      if (content === null) continue;
      const meta = document.createElement("meta");
      meta.name = name; meta.content = content; head.append(meta);
    }
    html.append(head, document.createElement("body"));
    // This changes only DOM nodes, not the loaded Document, URL/origin, or its
    // active response/meta CSP policies. Body handlers can alias Window handlers.
    document.documentElement.replaceWith(html);
    for (const name in window) {
      if (name.startsWith("on")) window[name] = null;
    }
    return { documentScripts: paths, metadata };
  };
  const prepared = await cdp.send("Runtime.evaluate", {
    expression: `(${prepare.toString()})(${JSON.stringify(endpoint)})`, returnByValue: true,
  });
  if (prepared.exceptionDetails) throw new Error("document-isolation-failed");
  const { documentScripts, metadata } = prepared.result.value;
  // Skipped scripts stay skipped. No site renderer/bootstrap/autoconnect code is executed.
  await cdp.send("Emulation.setScriptExecutionDisabled", { value: false });
  return {
    origin, documentPath: new URL(page.url()).pathname, documentHttpStatus: response.status(), documentScripts, metadata,
    enforcedCspPresent: response.headers()["content-security-policy"] !== undefined,
    reportOnlyCspPresent: response.headers()["content-security-policy-report-only"] !== undefined,
  };
}

export async function main(args = process.argv.slice(2)) {
  let options;
  try { options = parseOptions(args); } catch { console.error(HELP); return 1; }
  if (options.help) { console.log(HELP); return 0; }
  if (process.env.DEBUG || process.env.PWDEBUG || process.env.SSLKEYLOGFILE) {
    console.error("Remote probe refuses debug/protocol/TLS-key logging environments."); return 1;
  }
  const started = performance.now();
  const startedAt = new Date().toISOString();
  let server;
  let browser;
  let stage = "bundle";
  let output;
  const watchdog = setTimeout(() => {
    console.error(JSON.stringify({ ok: false, stage, code: "overall-90-second-deadline" }));
    server?.process().kill("SIGKILL");
    process.exit(1);
  }, 90_000);
  try {
    const bundle = await build({
      entryPoints: [fileURLToPath(new URL("remote-browser-probe-page.ts", import.meta.url))],
      bundle: true, platform: "browser", target: "es2022", format: "iife",
      globalName: "__WOVEN_REMOTE_PROBE__", write: false, sourcemap: false, logLevel: "silent",
    });
    stage = "browser-launch";
    const env = {};
    for (const key of ["PATH", "HOME", "DISPLAY", "XAUTHORITY", "WAYLAND_DISPLAY", "XDG_RUNTIME_DIR", "LANG"]) {
      if (process.env[key] !== undefined) env[key] = process.env[key];
    }
    server = await chromium.launchServer({ headless: !options.operatorInput, timeout: 10_000, env });
    browser = await chromium.connect(server.wsEndpoint(), { timeout: 10_000 });
    const context = await browser.newContext({
      ignoreHTTPSErrors: false, bypassCSP: false, serviceWorkers: "block", acceptDownloads: false,
    });
    const page = await context.newPage();
    stage = "document";
    const { metadata: documentMetadata, ...documentInfo } = await loadProbeDocument(
      page, options.pageUrl, options.run && !options.operatorInput,
      { browserJavaScript: bundle.outputFiles[0].text, endpoint: options.endpoint },
    );
    stage = "metadata";
    const metadata = { ...documentMetadata, ...await page.evaluate(() => ({
      secureContext: isSecureContext, webTransportAvailable: typeof globalThis.WebTransport === "function",
    })) };
    output = {
      mode: options.run ? "run" : "inspect", startedAt, browserVersion: browser.version(),
      ...documentInfo, metadata, resolvedTarget: options.target ?? metadata.target,
      channelId: options.channelId,
    };
    if (options.run) {
      stage = "probe";
      const timeoutMs = Math.floor(86_000 - (performance.now() - started));
      if (timeoutMs <= 0) throw new Error("setup-deadline");
      const result = await page.evaluate((config) => globalThis.__WOVEN_REMOTE_PROBE__.runRemoteProbe(config), {
        origin: options.origin, endpoint: options.endpoint, target: options.target,
        bootstrapPath: options.bootstrapPath, channelId: options.channelId,
        operatorInput: options.operatorInput, timeoutMs,
      });
      output.result = result;
    }
  } catch {
    output = { ...output, ok: false, stage, code: `${stage}-failed` };
  } finally {
    stage = "browser-teardown";
    let timeout;
    // Browser and Node share the same host clock: do not grant browser cleanup
    // another two seconds after the Woven close budget has already elapsed.
    const remaining = Math.max(0, (output?.result?.teardownStartedAt ?? Date.now()) + 2_000 - Date.now());
    try {
      if (remaining === 0) throw new Error("teardown-budget-exhausted");
      await Promise.race([
        (async () => { await browser?.close(); await server?.close(); })(),
        new Promise((_resolve, reject) => { timeout = setTimeout(() => reject(new Error("browser-close-deadline")), remaining); }),
      ]);
      output.browserTeardown = "closed";
    } catch {
      try {
        server?.process().kill("SIGKILL");
        output.browserTeardown = "forced";
      } catch {
        output = { ...output, ok: false, stage, code: "browser-teardown-failed" };
      }
    } finally { clearTimeout(timeout); clearTimeout(watchdog); }
  }
  output.endedAt = new Date().toISOString();
  output.overallElapsedMs = Math.round(performance.now() - started);
  console.log(JSON.stringify(output, null, 2));
  return output?.ok === false || output?.result?.ok === false ? 1 : 0;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) process.exitCode = await main();
