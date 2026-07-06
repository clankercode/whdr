/**
 * In-process mock whdr subscriber server for protocol-order unit tests.
 *
 * Uses `ws` in `noServer` mode behind a Node HTTP server so we can reject the
 * upgrade with a real HTTP 401 when the bearer token is wrong (exercising the
 * client's auth-failure path) and script exact frame orderings otherwise.
 */
import { createServer, type IncomingMessage, type Server } from "node:http";
import type { AddressInfo } from "node:net";

import { WebSocket, WebSocketServer } from "ws";

/** One accepted connection, with helpers to script frames and observe input. */
export interface MockConnection {
  readonly socket: WebSocket;
  readonly request: IncomingMessage;
  /** Client → server frames received so far (parsed JSON). */
  readonly received: unknown[];
  /** Send a server frame (object serialised to JSON text). */
  send(frame: unknown): void;
  /** Send a raw text frame verbatim (for malformed/unknown-frame tests). */
  sendRaw(text: string): void;
  /** Send a WebSocket-level ping (liveness; client must auto-pong). */
  pingWs(payload?: string): void;
  /** Close the underlying socket. */
  close(): void;
  /** Resolve once a client → server frame matching `pred` arrives. */
  waitFor(pred: (frame: unknown) => boolean): Promise<unknown>;
  /** Resolve once the client answers a WS ping with a pong. */
  waitForPong(): Promise<void>;
}

export interface MockServerOptions {
  /** Expected bearer token; a mismatch is rejected with HTTP 401. */
  token?: string;
  /** Called for each accepted connection (already past auth). */
  onConnection: (conn: MockConnection, connectionIndex: number) => void;
}

export class MockServer {
  private constructor(
    private readonly http: Server,
    private readonly wss: WebSocketServer,
    readonly url: string,
  ) {}

  static async start(options: MockServerOptions): Promise<MockServer> {
    const wss = new WebSocketServer({ noServer: true });
    const http = createServer();
    let index = 0;

    http.on("upgrade", (req, socket, head) => {
      if (options.token !== undefined) {
        const auth = req.headers["authorization"];
        if (auth !== `Bearer ${options.token}`) {
          socket.write("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
          socket.destroy();
          return;
        }
      }
      wss.handleUpgrade(req, socket, head, (ws) => {
        const conn = makeConnection(ws, req);
        options.onConnection(conn, index++);
      });
    });

    await new Promise<void>((resolve) => http.listen(0, "127.0.0.1", resolve));
    const address = http.address() as AddressInfo;
    const url = `ws://127.0.0.1:${address.port}/subscribe`;
    return new MockServer(http, wss, url);
  }

  async stop(): Promise<void> {
    for (const client of this.wss.clients) client.terminate();
    await new Promise<void>((resolve) => this.wss.close(() => resolve()));
    await new Promise<void>((resolve) => this.http.close(() => resolve()));
  }
}

function makeConnection(ws: WebSocket, request: IncomingMessage): MockConnection {
  const received: unknown[] = [];
  const frameWaiters: Array<{ pred: (f: unknown) => boolean; resolve: (f: unknown) => void }> =
    [];
  const pongWaiters: Array<() => void> = [];

  ws.on("message", (data: Buffer) => {
    let frame: unknown;
    try {
      frame = JSON.parse(data.toString("utf8"));
    } catch {
      return;
    }
    received.push(frame);
    for (let i = frameWaiters.length - 1; i >= 0; i--) {
      const waiter = frameWaiters[i]!;
      if (waiter.pred(frame)) {
        frameWaiters.splice(i, 1);
        waiter.resolve(frame);
      }
    }
  });
  ws.on("pong", () => {
    while (pongWaiters.length > 0) pongWaiters.shift()!();
  });

  return {
    socket: ws,
    request,
    received,
    send: (frame) => ws.send(JSON.stringify(frame)),
    sendRaw: (text) => ws.send(text),
    pingWs: (payload) => ws.ping(payload),
    close: () => ws.close(),
    waitFor: (pred) =>
      new Promise((resolve) => {
        const existing = received.find(pred);
        if (existing !== undefined) return resolve(existing);
        frameWaiters.push({ pred, resolve });
      }),
    waitForPong: () => new Promise((resolve) => pongWaiters.push(resolve)),
  };
}
