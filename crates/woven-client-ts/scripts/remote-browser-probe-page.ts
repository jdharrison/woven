import {
  AdmissionStatus, AuthenticationScheme, DeliveryClass, EnvelopeCodec, MessageKind,
  WovenClient, resolveWebTransportConstructor,
  type DecodedEnvelope, type WebTransport as WovenTransport,
} from "../src/index.js";
import { ControlPayload, EntityEnteredPayload } from "../generated/woven/protocol/v1.js";

const MAX_U64 = 18_446_744_073_709_551_615n;
const TARGET_KEYS = ["version", "url", "namespaceId", "sessionId", "spaceId", "spaceEpoch"];
const OPERATION_MS = 10_000;
const CLOSE_MS = 2_000;
export const MAX_STARTUP_ENVELOPES = 64;

export type Target = {
  version: 1;
  url: string;
  namespaceId: string;
  sessionId: string;
  spaceId: string;
  spaceEpoch: string;
};
export type ProbeOptions = {
  origin: string;
  endpoint: string;
  target?: Target;
  bootstrapPath?: string;
  channelId: string;
  operatorInput: boolean;
  timeoutMs: number;
};
type ProbeClient = Pick<WovenClient, "requestAdmission" | "subscribeSpace" | "recvTimeout" | "closeGracefully">;
export type ProbeConnection = {
  client: ProbeClient;
  stop: () => void;
  outboundKinds: MessageKind[];
};
export type ProbeReport = {
  ok: boolean;
  stage: string;
  code: string;
  authenticated: boolean;
  admitted: boolean;
  subscriptionAccepted: boolean;
  entityId: string | null;
  startupEnvelopes: number;
  close: "not-opened" | "closed" | "failed";
  teardownStartedAt: number | null;
  elapsedMs: number;
  outboundKinds: number[];
};

class ProbeFailure extends Error {
  constructor(
    readonly code: string, readonly close?: ProbeReport["close"],
    readonly outboundKinds?: MessageKind[], readonly teardownStartedAt?: number,
  ) { super(code); }
}
function fail(code: string): never { throw new ProbeFailure(code); }
function failureCode(error: unknown, fallback: string): string {
  // Neither remote error messages nor bootstrap bodies are diagnostic output.
  return error instanceof ProbeFailure ? error.code : fallback;
}

export function positiveId(value: unknown): string {
  if (typeof value !== "string" || !/^[1-9][0-9]{0,19}$/.test(value) || BigInt(value) > MAX_U64) {
    fail("invalid-scope-id");
  }
  return value;
}
export function httpsUrl(value: unknown): string {
  if (typeof value !== "string" || value.length > 512 || /[\s\\]/u.test(value)) fail("invalid-https-url");
  let url: URL;
  try { url = new URL(value); } catch { return fail("invalid-https-url"); }
  if (!value.startsWith("https://") || url.protocol !== "https:" || url.username || url.password ||
      url.search || url.hash || value.includes("?") || value.includes("#")) fail("invalid-https-url");
  return url.href;
}
function object(text: string): Record<string, unknown> {
  if (new TextEncoder().encode(text).byteLength > 8_192) fail("descriptor-too-large");
  let value: unknown;
  try { value = JSON.parse(text); } catch { return fail("invalid-descriptor"); }
  if (value === null || typeof value !== "object" || Array.isArray(value)) fail("invalid-descriptor");
  return value as Record<string, unknown>;
}
function exactKeys(value: Record<string, unknown>, keys: string[]): void {
  if (Object.keys(value).length !== keys.length || keys.some((key) => !Object.hasOwn(value, key))) {
    fail("descriptor-fields-mismatch");
  }
}
export function validateTarget(value: Record<string, unknown>, endpoint: string): Target {
  exactKeys(value, TARGET_KEYS);
  if (value.version !== 1) fail("unsupported-descriptor-version");
  const url = httpsUrl(value.url);
  if (url !== httpsUrl(endpoint) || new URL(url).pathname === "/") fail("endpoint-mismatch");
  return {
    version: 1, url,
    namespaceId: positiveId(value.namespaceId), sessionId: positiveId(value.sessionId),
    spaceId: positiveId(value.spaceId), spaceEpoch: positiveId(value.spaceEpoch),
  };
}
export function canonicalBootstrapPath(path: string, origin: string): string {
  if (!path.startsWith("/") || path.startsWith("//") || path.length > 512 || /[\s\\?#]/u.test(path)) {
    fail("invalid-bootstrap-path");
  }
  const url = new URL(path, origin);
  if (url.origin !== origin || url.pathname !== path) fail("invalid-bootstrap-path");
  return url.href;
}
function configuredPath(doc: Document): string {
  return doc.querySelector<HTMLMetaElement>('meta[name="weaver-lobby-bootstrap"]')?.content ?? "";
}
function configuredTarget(doc: Document): string {
  return doc.querySelector<HTMLMetaElement>('meta[name="weaver-lobby-target"]')?.content ?? "";
}
export function inspectDocument(doc: Document, origin: string, endpoint: string) {
  const path = configuredPath(doc);
  const targetText = configuredTarget(doc);
  let target: Target | null = null;
  let bootstrapValid = false;
  try { target = validateTarget(object(targetText), endpoint); } catch { /* Only validated metadata leaves the browser. */ }
  if (path !== "" && path !== "off") {
    try { canonicalBootstrapPath(path, origin); bootstrapValid = true; } catch { /* Invalid values are not printed. */ }
  }
  const framePaths: string[] = [];
  let inspectedFrames = 0;
  for (const frame of doc.querySelectorAll<HTMLIFrameElement>("iframe[src]")) {
    if (inspectedFrames++ >= 8) break;
    try {
      const url = new URL(frame.src);
      if (url.origin === origin && !url.search && !url.hash && !url.username && !url.password &&
          /^\/[a-zA-Z0-9_./-]{0,200}$/.test(url.pathname) && !/[0-9a-f]{32}/i.test(url.pathname)) {
        framePaths.push(url.pathname);
      }
    } catch { /* No unvalidated URLs in output. */ }
  }
  return {
    bootstrapConfigured: path !== "" && path !== "off", bootstrapValid,
    targetConfigured: targetText !== "", targetValid: target !== null, target, framePaths,
    metaCspPresent: doc.querySelector('meta[http-equiv="Content-Security-Policy" i]') !== null,
  };
}

export async function bounded<T>(promise: Promise<T>, signal: AbortSignal, timeoutMs: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let cancel: () => void = () => {};
  try {
    return await Promise.race([promise, new Promise<never>((_resolve, reject) => {
      cancel = () => reject(new ProbeFailure("cancelled-or-overall-deadline"));
      signal.addEventListener("abort", cancel, { once: true });
      if (signal.aborted) cancel();
      timer = setTimeout(() => reject(new ProbeFailure("operation-deadline")), timeoutMs);
    })]);
  } finally {
    clearTimeout(timer);
    signal.removeEventListener("abort", cancel);
  }
}

export async function fetchBootstrap(
  path: string, target: Target, origin: string, signal: AbortSignal, fetcher: typeof fetch = fetch,
): Promise<string> {
  const controller = new AbortController();
  const cancel = () => controller.abort();
  signal.addEventListener("abort", cancel, { once: true });
  if (signal.aborted) cancel();
  const timer = setTimeout(cancel, OPERATION_MS);
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  try {
    const response = await bounded(fetcher(canonicalBootstrapPath(path, origin), {
      signal: controller.signal, credentials: "omit", cache: "no-store", redirect: "error",
      referrerPolicy: "no-referrer", headers: { Accept: "application/json" },
    }), controller.signal, OPERATION_MS);
    if (!response.ok || response.redirected) fail("bootstrap-http-failed");
    if (response.headers.get("content-type")?.split(";")[0].trim().toLowerCase() !== "application/json") {
      fail("bootstrap-not-json");
    }
    const cache = response.headers.get("cache-control") ?? "";
    if (!/(?:^|,)\s*no-store\s*(?:,|$)/i.test(cache) || !/(?:^|,)\s*private\s*(?:,|$)/i.test(cache)) {
      fail("bootstrap-cache-policy-unsafe");
    }
    if (Number(response.headers.get("content-length")) > 8_192 || !response.body) fail("bootstrap-body-invalid");
    reader = response.body.getReader();
    const bytes = new Uint8Array(8_192);
    let length = 0;
    let reads = 0;
    for (;;) {
      if (reads++ >= 256) fail("bootstrap-read-bound-exceeded");
      const chunk = await bounded(reader.read(), controller.signal, OPERATION_MS);
      if (chunk.done) break;
      if (length + chunk.value.byteLength > bytes.length) fail("bootstrap-too-large");
      bytes.set(chunk.value, length);
      length += chunk.value.byteLength;
    }
    const value = object(new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(0, length)));
    exactKeys(value, [...TARGET_KEYS, "token"]);
    const { token, ...fields } = value;
    const actual = validateTarget(fields, target.url);
    if (TARGET_KEYS.some((key) => actual[key as keyof Target] !== target[key as keyof Target])) {
      fail("bootstrap-target-mismatch");
    }
    if (typeof token !== "string" || !/^[0-9a-f]{64}$/.test(token)) fail("invalid-bearer");
    return token;
  } finally {
    clearTimeout(timer);
    signal.removeEventListener("abort", cancel);
    controller.abort();
    void reader?.cancel().catch(() => {});
  }
}

async function openVerifiedConnection(
  target: Target, token: string, signal: AbortSignal,
): Promise<ProbeConnection> {
  if (signal.aborted) fail("cancelled-or-overall-deadline");
  const Transport = resolveWebTransportConstructor();
  const transport: WovenTransport = new Transport(target.url, { requireUnreliable: true });
  const outboundKinds: MessageKind[] = [];
  const expected = [MessageKind.Hello, MessageKind.Authenticate, MessageKind.RequestAdmission, MessageKind.SubscribeSpace];
  const stop = () => transport.close({ closeCode: 0, reason: "bounded remote probe stopped" });
  const onAbort = () => stop();
  signal.addEventListener("abort", onAbort, { once: true });
  const deadline = performance.now() + OPERATION_MS;
  const remaining = () => Math.max(1, deadline - performance.now());
  let stage = "webtransport-ready";
  try {
    await bounded(transport.ready, signal, remaining());
    stage = "stream-creation";
    const stream = await bounded(transport.createBidirectionalStream(), signal, remaining());
    const codec = new EnvelopeCodec();
    const writable = new WritableStream<Uint8Array>({
      async write(bytes) {
        const kind = codec.decode(bytes).messageKind;
        if (kind !== expected[outboundKinds.length]) fail("unexpected-outbound-message");
        outboundKinds.push(kind);
        const writer = stream.writable.getWriter();
        try { await writer.write(bytes); } finally { writer.releaseLock(); }
      },
    });
    stage = "wvn1-authentication";
    const client = await bounded(WovenClient.fromTransport(transport, { readable: stream.readable, writable }, {
      url: target.url, token, authenticationScheme: AuthenticationScheme.Bearer,
      connectTimeoutMs: Math.max(1, Math.ceil(remaining())),
      webTransportOptions: { requireUnreliable: true }, datagramMaxAgeMs: 250,
    }), signal, remaining());
    return { client, outboundKinds, stop: () => { signal.removeEventListener("abort", onAbort); stop(); } };
  } catch (error) {
    signal.removeEventListener("abort", onAbort);
    const teardownStartedAt = Date.now();
    stop();
    // The caller cannot closeGracefully before SDK construction succeeds.
    let close: ProbeReport["close"] = "closed";
    try { await bounded(transport.closed, new AbortController().signal, CLOSE_MS); } catch { close = "failed"; }
    throw new ProbeFailure(failureCode(error, `${stage}-failed`), close, outboundKinds, teardownStartedAt);
  }
}

export async function probeConnection(
  target: Target, token: string, channelId: string, signal: AbortSignal,
  open: typeof openVerifiedConnection = openVerifiedConnection,
): Promise<ProbeReport> {
  const started = performance.now();
  const report: ProbeReport = {
    ok: false, stage: "authentication", code: "not-started", authenticated: false, admitted: false,
    subscriptionAccepted: false, entityId: null, startupEnvelopes: 0, close: "not-opened",
    teardownStartedAt: null, elapsedMs: 0, outboundKinds: [],
  };
  let connection: ProbeConnection | undefined;
  try {
    connection = await open(target, token, signal);
    token = "";
    report.authenticated = true;
    report.stage = "admission";
    const ns = BigInt(target.namespaceId), session = BigInt(target.sessionId);
    const space = BigInt(target.spaceId), epoch = BigInt(target.spaceEpoch), channel = BigInt(channelId);
    const outcome = await bounded(connection.client.requestAdmission(ns, session, 1n, crypto.randomUUID()), signal, OPERATION_MS);
    if (outcome.status !== AdmissionStatus.Admitted) {
      fail(outcome.status === AdmissionStatus.Queued ? "admission-queued-not-success" : "admission-not-admitted");
    }
    report.admitted = true;
    report.stage = "subscription";
    await bounded(connection.client.subscribeSpace(ns, session, space, epoch, channel), signal, OPERATION_MS);
    const deadline = performance.now() + OPERATION_MS;
    while (report.startupEnvelopes < MAX_STARTUP_ENVELOPES && performance.now() < deadline) {
      const envelope: DecodedEnvelope | null = await bounded(
        connection.client.recvTimeout(Math.min(1_000, Math.max(1, Math.ceil(deadline - performance.now())))),
        signal, Math.max(1, deadline - performance.now()),
      );
      if (envelope === null) continue;
      report.startupEnvelopes += 1;
      if (envelope.messageKind === MessageKind.ProtocolError || envelope.messageKind === MessageKind.SubscriptionRejected) {
        fail("subscription-rejected");
      }
      if (envelope.namespaceId !== ns || envelope.sessionId !== session || envelope.spaceId !== space || envelope.spaceEpoch !== epoch) {
        fail("startup-scope-mismatch");
      }
      if (envelope.deliveryClass !== DeliveryClass.ReliableOrdered) fail("startup-delivery-mismatch");
      if (envelope.messageKind === MessageKind.SubscriptionAccepted) {
        if (envelope.channelId !== channel) fail("subscription-channel-mismatch");
        const ack = envelope.control as { subscriptionId?: () => bigint; acceptedSpaceEpoch?: () => bigint } | null;
        if (typeof ack?.subscriptionId !== "function" || ack.subscriptionId() <= 0n ||
            typeof ack.acceptedSpaceEpoch !== "function" || ack.acceptedSpaceEpoch() !== epoch) {
          fail("subscription-payload-mismatch");
        }
        report.subscriptionAccepted = true;
      } else if (envelope.messageKind === MessageKind.EntityEntered) {
        const entity = envelope.control;
        if (!report.subscriptionAccepted || envelope.entityId === null || envelope.entityId <= 0n ||
            envelope.controlType !== ControlPayload.EntityEnteredPayload || !(entity instanceof EntityEnteredPayload) ||
            entity.ownerEntityId() <= 0n || entity.ownerEntityId() !== envelope.entityId) {
          fail("entity-assignment-invalid");
        }
        report.entityId = envelope.entityId.toString();
        break;
      }
    }
    if (!report.subscriptionAccepted || report.entityId === null) fail("startup-bound-exceeded");
    report.code = "admitted-and-subscribed";
  } catch (error) {
    report.code = failureCode(error, `${report.stage}-failed`);
    if (error instanceof ProbeFailure && error.close) {
      report.close = error.close;
      report.teardownStartedAt = error.teardownStartedAt ?? null;
      report.outboundKinds = error.outboundKinds?.slice(0, 4) ?? [];
    }
  } finally {
    token = "";
    report.teardownStartedAt ??= Date.now();
    if (connection) {
      report.outboundKinds = connection.outboundKinds.slice(0, 4);
      try {
        await bounded(connection.client.closeGracefully(CLOSE_MS), new AbortController().signal, CLOSE_MS);
        report.close = "closed";
      } catch {
        report.close = "failed";
        report.code = "teardown-failed";
      } finally { connection.stop(); }
    }
    report.elapsedMs = Math.round(performance.now() - started);
    report.ok = report.code === "admitted-and-subscribed" && report.close === "closed";
    if (report.ok) report.stage = "complete";
  }
  return report;
}

async function operatorCredential(target: Target, signal: AbortSignal): Promise<string> {
  const panel = document.createElement("dialog");
  const description = document.createElement("p");
  description.textContent = `Trusted Woven probe · ${location.origin} → ${target.url} · namespace ${target.namespaceId}, session ${target.sessionId}, space ${target.spaceId}, epoch ${target.spaceEpoch}. One attempt; no application publishes or logs. Paste only the session Bearer, never an owner/admin credential.`;
  const input = document.createElement("input");
  input.type = "password"; input.autocomplete = "off"; input.maxLength = 64;
  input.setAttribute("aria-label", "Session Bearer (memory only)");
  const button = document.createElement("button"); button.textContent = "Run one probe";
  panel.append(description, input, button);
  document.body.append(panel); panel.showModal(); input.focus();
  try {
    return await bounded(new Promise<string>((resolve) => {
      button.addEventListener("click", () => {
        if (!/^[0-9a-f]{64}$/.test(input.value)) { input.setCustomValidity("Expected a managed session Bearer."); input.reportValidity(); return; }
        const token = input.value; input.value = ""; button.disabled = true; resolve(token);
      }, { once: false });
      panel.addEventListener("cancel", () => { input.value = ""; resolve(""); }, { once: true });
    }), signal, 85_000);
  } finally { input.value = ""; panel.remove(); }
}

export async function runRemoteProbe(options: ProbeOptions): Promise<ProbeReport> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), Math.min(86_000, Math.max(1, options.timeoutMs)));
  const started = performance.now();
  let stage = "configuration";
  let token = "";
  try {
    if (location.origin !== options.origin || !isSecureContext) fail("document-origin-or-security-mismatch");
    if (typeof globalThis.WebTransport !== "function") fail("webtransport-unavailable");
    const target = options.target ? validateTarget(options.target, options.endpoint) :
      validateTarget(object(configuredTarget(document)), options.endpoint);
    positiveId(options.channelId);
    stage = "credential";
    token = options.operatorInput ? await operatorCredential(target, controller.signal) :
      await fetchBootstrap(options.bootstrapPath ?? configuredPath(document), target, options.origin, controller.signal);
    if (!token) fail("operator-cancelled");
    const result = await probeConnection(target, token, options.channelId, controller.signal);
    result.elapsedMs = Math.round(performance.now() - started);
    return result;
  } catch (error) {
    return {
      ok: false, stage, code: failureCode(error, `${stage}-failed`), authenticated: false, admitted: false,
      subscriptionAccepted: false, entityId: null, startupEnvelopes: 0, close: "not-opened",
      teardownStartedAt: Date.now(), elapsedMs: Math.round(performance.now() - started), outboundKinds: [],
    };
  } finally { token = ""; clearTimeout(timer); controller.abort(); }
}
