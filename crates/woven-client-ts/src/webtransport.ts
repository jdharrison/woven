/**
 * Minimal WHATWG WebTransport interfaces, declared locally so the client does
 * not depend on a host's `lib.dom.d.ts`. In the browser these resolve to the
 * standard global `WebTransport` classes.
 */

export interface WebTransportError extends Error {
  source: "stream" | "session";
  streamErrorCode: number | null;
}

export interface WebTransportCloseInfo {
  closeCode?: number;
  reason?: string;
}

export interface WebTransportBidirectionalStream {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
}

export interface WebTransportDatagramDuplexStream {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
  readonly incomingMaxAge: number | null;
  readonly outgoingMaxAge: number | null;
  readonly incomingHighWaterMark: number;
  readonly outgoingHighWaterMark: number;
}

export interface WebTransportHash {
  algorithm: "sha-256";
  value: BufferSource;
}

const MAX_SERVER_CERTIFICATE_HASHES = 8;

export interface WebTransportOptions {
  allowPooling?: boolean;
  requireUnreliable?: boolean;
  serverCertificateHashes?: readonly WebTransportHash[];
  congestionControl?: "default" | "throughput";
}

/** Validate and defensively copy options before passing them to the runtime constructor. */
export function normalizeWebTransportOptions(
  options: WebTransportOptions | undefined,
): WebTransportOptions | undefined {
  if (options === undefined) return undefined;
  if (
    options.serverCertificateHashes !== undefined &&
    options.serverCertificateHashes.length > MAX_SERVER_CERTIFICATE_HASHES
  ) {
    throw new Error(
      `WebTransport server certificate hashes cannot exceed ${MAX_SERVER_CERTIFICATE_HASHES}`,
    );
  }
  const hashes = options.serverCertificateHashes?.map((hash) => {
    if (hash.algorithm !== "sha-256") {
      throw new Error("WebTransport server certificate hashes must use sha-256");
    }
    const bytes = hashBytes(hash.value);
    if (bytes.byteLength !== 32) {
      throw new Error("WebTransport sha-256 server certificate hashes must be 32 bytes");
    }
    return { algorithm: "sha-256" as const, value: bytes.slice() };
  });
  return {
    allowPooling: options.allowPooling,
    requireUnreliable: options.requireUnreliable,
    serverCertificateHashes: hashes,
    congestionControl: options.congestionControl,
  };
}

function hashBytes(value: BufferSource): Uint8Array {
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
}

/**
 * The subset of the WHATWG `WebTransport` interface used by the client.
 */
export interface WebTransport {
  readonly ready: Promise<void>;
  readonly closed: Promise<WebTransportCloseInfo>;
  readonly datagrams: WebTransportDatagramDuplexStream;
  createBidirectionalStream(): Promise<WebTransportBidirectionalStream>;
  close(info?: WebTransportCloseInfo): void;
}

export interface WebTransportConstructor {
  new (url: string, options?: WebTransportOptions): WebTransport;
}

/**
 * Resolve the global `WebTransport` constructor at runtime, or throw when the
 * environment does not provide it (for example plain Node without a shim).
 */
export function resolveWebTransportConstructor(): WebTransportConstructor {
  const globalObject = globalThis as unknown as {
    WebTransport?: WebTransportConstructor;
  };
  if (typeof globalObject.WebTransport !== "function") {
    throw new Error(
      "WebTransport is not available in this environment. " +
        "Woven's TypeScript client requires a browser (or runtime shim) " +
        "that implements the WHATWG WebTransport API.",
    );
  }
  return globalObject.WebTransport;
}
