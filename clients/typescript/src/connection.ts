/**
 * The typed WebSocket connection: authenticated upgrade, `welcome` handshake,
 * and a pull-based typed frame stream over the event-driven `ws` socket.
 *
 * `ws` answers server ping frames with pong automatically (conformance item 9),
 * so liveness needs no code here.
 */
import { WebSocket, type ClientOptions } from "ws";

import {
  AuthError,
  ConnectionClosedError,
  HttpError,
  RequestError,
  WhdrError,
} from "./errors.js";
import {
  parseServerFrame,
  subscribeFrame,
  type ClientFrame,
  type ServerFrame,
} from "./frames.js";

/** Build the `Authorization: Bearer <token>` header (conformance item 1). */
export function buildHeaders(token: string): Record<string, string> {
  return { Authorization: `Bearer ${token}` };
}

/**
 * An authenticated subscriber connection, positioned just after the `welcome`
 * frame. {@link Connection.recv} yields typed {@link ServerFrame}s, skipping
 * unrecognised frames; the underlying `ws` socket auto-answers pings.
 */
export class Connection {
  #name = "";
  readonly #buffer: ServerFrame[] = [];
  #terminal: WhdrError | null = null;
  #wakers: Array<() => void> = [];

  private constructor(private readonly ws: WebSocket) {
    ws.on("message", (data: unknown, isBinary: boolean) => {
      if (isBinary) return; // ignore binary frames
      const text = Buffer.isBuffer(data)
        ? data.toString("utf8")
        : Array.isArray(data)
          ? Buffer.concat(data as Buffer[]).toString("utf8")
          : String(data);
      const frame = parseServerFrame(text);
      if (frame !== null) {
        this.#buffer.push(frame);
        this.#wake();
      }
      // Unknown frame: ignore, keep reading (conformance item 10).
    });
    ws.on("close", () => {
      this.#terminal ??= new ConnectionClosedError();
      this.#wake();
    });
    ws.on("error", (err: Error) => {
      this.#terminal ??= new ConnectionClosedError(
        `websocket transport error: ${err.message}`,
        err,
      );
      this.#wake();
    });
  }

  /**
   * Connect, authenticate, and consume the `welcome` frame (conformance item
   * 2). A `401` upgrade rejection maps to {@link AuthError} (item 1); other
   * statuses to {@link HttpError}.
   */
  static async connect(
    url: string,
    token: string,
    options: ClientOptions = {},
  ): Promise<Connection> {
    let ws: WebSocket;
    try {
      ws = new WebSocket(url, { ...options, headers: buildHeaders(token) });
    } catch (err) {
      throw new RequestError(err instanceof Error ? err.message : String(err));
    }

    const conn = new Connection(ws);
    await new Promise<void>((resolve, reject) => {
      const onUnexpected = (
        _req: unknown,
        res: { statusCode?: number },
      ): void => {
        cleanup();
        ws.terminate();
        const status = res.statusCode ?? 0;
        reject(status === 401 ? new AuthError() : new HttpError(status));
      };
      const onOpen = (): void => {
        cleanup();
        resolve();
      };
      const onError = (err: Error): void => {
        cleanup();
        // A pre-open error with no HTTP response: transient transport failure.
        reject(new ConnectionClosedError(`connection failed: ${err.message}`, err));
      };
      const cleanup = (): void => {
        ws.off("open", onOpen);
        ws.off("error", onError);
        ws.off("unexpected-response", onUnexpected);
      };
      ws.once("open", onOpen);
      ws.once("error", onError);
      ws.once("unexpected-response", onUnexpected);
    });

    // Read frames until the welcome; anything before it is skipped.
    for (;;) {
      const frame = await conn.recv();
      if (frame.type === "welcome") {
        conn.#name = frame.name;
        return conn;
      }
      // Unexpected pre-welcome frame; ignore and keep reading.
    }
  }

  /** The subscriber name echoed in the `welcome` frame (the token's label). */
  get name(): string {
    return this.#name;
  }

  /**
   * Send a `subscribe`, optionally resuming from `afterSeq` (conformance item
   * 3: always resume with `replay.after_seq = cursor`). Pass `undefined` for
   * live-only.
   */
  async subscribe(patterns: string[], afterSeq: number | undefined): Promise<void> {
    await this.send(subscribeFrame(patterns, afterSeq));
  }

  /** Send an application-level `ping`. */
  async ping(): Promise<void> {
    await this.send({ type: "ping" });
  }

  /** Send a client frame. */
  send(frame: ClientFrame): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.ws.send(JSON.stringify(frame), (err) => {
        if (err) reject(new ConnectionClosedError(`send failed: ${err.message}`, err));
        else resolve();
      });
    });
  }

  /**
   * Read the next typed server frame. Skips unrecognised frames (item 10) and
   * throws {@link ConnectionClosedError} when the peer closes or the transport
   * errors.
   */
  async recv(): Promise<ServerFrame> {
    for (;;) {
      const frame = this.#buffer.shift();
      if (frame !== undefined) return frame;
      if (this.#terminal !== null) throw this.#terminal;
      await new Promise<void>((resolve) => this.#wakers.push(resolve));
    }
  }

  /** Close the underlying socket. Idempotent. */
  close(): void {
    this.#terminal ??= new ConnectionClosedError();
    try {
      this.ws.close();
    } catch {
      this.ws.terminate();
    }
    this.#wake();
  }

  #wake(): void {
    const wakers = this.#wakers;
    this.#wakers = [];
    for (const wake of wakers) wake();
  }
}
