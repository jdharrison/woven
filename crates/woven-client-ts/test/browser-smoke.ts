import {
  AdmissionStatus,
  AuthenticationScheme,
  MessageKind,
  QueueState,
  WovenClient,
  type ManagedAdmissionOutcome,
} from "../src/index.js";

type BrowserConfig = {
  url: string;
  certificateSha256: string;
  token: string;
  namespaceId: string;
  sessionId: string;
  iteration: number;
};

type BrowserResult =
  | { ok: true; entityId: string; elapsedMs: number }
  | { ok: false; error: string };

declare global {
  interface Window {
    __WOVEN_BROWSER_CONFIG__: BrowserConfig;
    __WOVEN_BROWSER_RESULT__?: BrowserResult;
  }
}

function certificateHash(hex: string): ArrayBuffer {
  if (!/^[0-9a-f]{64}$/.test(hex)) throw new Error("invalid certificate fingerprint");
  const bytes = new Uint8Array(32);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes.buffer;
}

function admitted(outcome: ManagedAdmissionOutcome): boolean {
  return outcome.kind === "admission"
    ? outcome.result.status === AdmissionStatus.Admitted
    : outcome.update.state === QueueState.Admitted;
}

async function receiveKind(client: WovenClient, kind: MessageKind): Promise<Awaited<ReturnType<WovenClient["recv"]>>> {
  for (let count = 0; count < 16; count += 1) {
    const envelope = await client.recvTimeout(5_000);
    if (envelope === null) throw new Error("timed out waiting for Woven envelope");
    if (envelope.messageKind === MessageKind.ProtocolError) {
      throw new Error("Woven returned ProtocolError");
    }
    if (envelope.messageKind === kind) return envelope;
  }
  throw new Error("bounded receive limit reached");
}

async function run(): Promise<void> {
  const started = performance.now();
  const config = window.__WOVEN_BROWSER_CONFIG__;
  let client: WovenClient | undefined;
  try {
    if (typeof WebTransport !== "function") throw new Error("browser WebTransport unavailable");
    const namespaceId = BigInt(config.namespaceId);
    const sessionId = BigInt(config.sessionId);
    client = await WovenClient.connect({
      url: config.url,
      token: config.token,
      authenticationScheme: AuthenticationScheme.Bearer,
      connectTimeoutMs: 10_000,
      webTransportOptions: {
        serverCertificateHashes: [
          { algorithm: "sha-256", value: certificateHash(config.certificateSha256) },
        ],
      },
    });
    const outcome = await client.admitWithCancellation(
      namespaceId,
      sessionId,
      `browser-smoke-${config.iteration}-${crypto.randomUUID()}`,
      15_000,
      new AbortController().signal,
    );
    if (!admitted(outcome)) throw new Error("managed admission did not join the session");

    await client.subscribeSpace(namespaceId, sessionId, 1n, 1n, 1n);
    await receiveKind(client, MessageKind.SubscriptionAccepted);
    const entered = await receiveKind(client, MessageKind.EntityEntered);
    if (entered.entityId === null) throw new Error("EntityEntered omitted entity ID");

    const payload = Uint8Array.of(0x57, 0x56, 0x4e, config.iteration & 0xff);
    await client.publishEvent(
      namespaceId,
      sessionId,
      1n,
      1n,
      1n,
      entered.entityId,
      1n,
      1n,
      payload,
    );
    const echoed = await receiveKind(client, MessageKind.ReliableEvent);
    if (
      echoed.entityId !== entered.entityId ||
      echoed.senderSequence !== 1n ||
      echoed.payload === null ||
      echoed.payload.length !== payload.length ||
      !echoed.payload.every((byte, index) => byte === payload[index])
    ) {
      throw new Error("published event echo did not match");
    }

    await client.closeGracefully(2_000, 0, "browser smoke complete");
    client = undefined;
    window.__WOVEN_BROWSER_RESULT__ = {
      ok: true,
      entityId: entered.entityId.toString(),
      elapsedMs: performance.now() - started,
    };
  } catch (error) {
    client?.close(0, "browser smoke failed");
    const raw = error instanceof Error ? error.message : String(error);
    window.__WOVEN_BROWSER_RESULT__ = {
      ok: false,
      error: raw.replaceAll(config.token, "[REDACTED]"),
    };
  }
}

void run();
