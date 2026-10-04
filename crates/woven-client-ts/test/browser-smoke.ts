import {
  AdmissionStatus,
  AuthenticationScheme,
  DeliveryClass,
  MessageKind,
  QueueState,
  WovenClient,
  type ManagedAdmissionOutcome,
  type DecodedEnvelope,
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
  | {
      ok: true;
      entityId: string;
      elapsedMs: number;
      datagramAttempts: number;
      datagramEchoSequence: string;
      reliablePoseWrites: number;
    }
  | { ok: false; error: string; datagramAttempts: number; reliablePoseWrites: number };

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
  let datagramAttempts = 0;
  let reliablePoseWrites = 0;
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
        requireUnreliable: true,
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

    // Opaque 25-byte sample pose fixture, not a Woven server domain codec.
    const poseBytes = Uint8Array.of(
      0x01,
      0x00, 0x00, 0xa0, 0x3f,
      0x00, 0x00, 0x20, 0x40,
      0x00, 0x00, 0x70, 0xc0,
      0x00, 0x00, 0x80, 0x3e,
      0x00, 0x00, 0x00, 0xbf,
      0x00, 0x00, 0x40, 0x3f,
    );
    if (poseBytes.byteLength !== 25) throw new Error("invalid opaque pose fixture length");

    // Observe the public stream API, including any accidental internal reliable fallback.
    const controlWritable = client.stream.writable;
    const getControlWriter = controlWritable.getWriter;
    controlWritable.getWriter = () => {
      const writer = getControlWriter.call(controlWritable);
      const write = writer.write.bind(writer);
      writer.write = (chunk) => {
        reliablePoseWrites += 1;
        return write(chunk);
      };
      return writer;
    };
    let datagramEcho: DecodedEnvelope | undefined;
    let datagramError: unknown;
    try {
      const receive = client.recvDatagramTimeout(5_000).then((envelope) => {
        if (envelope === null) {
          throw new Error(`no datagram echo within 5 seconds after ${datagramAttempts} attempts`);
        }
        if (
          envelope.messageKind !== MessageKind.EntityState ||
          envelope.deliveryClass !== DeliveryClass.UnreliableSequenced ||
          envelope.namespaceId !== namespaceId ||
          envelope.sessionId !== sessionId ||
          envelope.spaceId !== 1n ||
          envelope.spaceEpoch !== 1n ||
          envelope.channelId !== 4n ||
          envelope.entityId !== entered.entityId ||
          envelope.payloadTypeId !== 1n ||
          envelope.senderSequence < 1n ||
          envelope.senderSequence > BigInt(datagramAttempts) ||
          envelope.payload === null ||
          envelope.payload.byteLength !== 25 ||
          !envelope.payload.every((byte, index) => byte === poseBytes[index])
        ) {
          throw new Error("opaque datagram echo kind, delivery, scope, sequence, or bytes did not match");
        }
        datagramEcho = envelope;
      }).catch((error: unknown) => { datagramError = error; });

      const sendDeadline = performance.now() + 2_000;
      while (datagramAttempts < 20 && performance.now() < sendDeadline) {
        if (datagramEcho !== undefined) break;
        if (datagramError !== undefined) throw datagramError;
        datagramAttempts += 1;
        await client.publishUnreliableState(
          namespaceId, sessionId, 1n, 1n, 4n, entered.entityId,
          BigInt(datagramAttempts), 1n, poseBytes,
        );
        // A write is only an attempt: stop early only after validating an actual received echo.
        if (datagramEcho !== undefined) break;
        const waitMs = Math.min(100, Math.max(0, sendDeadline - performance.now()));
        await new Promise<void>((resolve) => setTimeout(resolve, waitMs));
      }
      await receive;
      if (datagramError !== undefined) throw datagramError;
      if (datagramEcho === undefined) throw new Error("no actual datagram echo received");
      if (reliablePoseWrites !== 0) throw new Error("pose phase wrote to the reliable control stream");
    } finally {
      controlWritable.getWriter = getControlWriter;
    }

    // Reliable channel 1 must still echo after independently exercising the datagram lane.
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
      echoed.deliveryClass !== DeliveryClass.ReliableOrdered ||
      echoed.namespaceId !== namespaceId ||
      echoed.sessionId !== sessionId ||
      echoed.spaceId !== 1n ||
      echoed.spaceEpoch !== 1n ||
      echoed.channelId !== 1n ||
      echoed.entityId !== entered.entityId ||
      echoed.payloadTypeId !== 1n ||
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
      datagramAttempts,
      datagramEchoSequence: datagramEcho.senderSequence.toString(),
      reliablePoseWrites,
    };
  } catch (error) {
    client?.close(0, "browser smoke failed");
    const raw = error instanceof Error ? error.message : String(error);
    window.__WOVEN_BROWSER_RESULT__ = {
      ok: false,
      error: raw.replaceAll(config.token, "[REDACTED]"),
      datagramAttempts,
      reliablePoseWrites,
    };
  }
}

void run();
