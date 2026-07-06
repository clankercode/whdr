/**
 * Error taxonomy for the subscriber client.
 *
 * {@link WhdrError.fatal} distinguishes *fatal* errors (the {@link
 * WhdrSubscriber.run} loop stops and rethrows) from *transient* ones (the loop
 * reconnects with backoff). Per the appendix, an auth failure, a `revoked`
 * close, a handler error, and a cursor-store failure are fatal; a dropped
 * socket, a `shutdown` close, and a `lagged` eviction are transient.
 */
export abstract class WhdrError extends Error {
  /** Whether the run loop should stop and rethrow rather than reconnect. */
  abstract readonly fatal: boolean;
}

/** The WebSocket upgrade was rejected with HTTP 401. Token wrong/absent. Fatal. */
export class AuthError extends WhdrError {
  override readonly name = "AuthError";
  readonly fatal = true;
  constructor() {
    super("authentication failed (HTTP 401): token missing, wrong, or revoked");
  }
}

/** The WebSocket upgrade failed with a non-401 HTTP status. Transient. */
export class HttpError extends WhdrError {
  override readonly name = "HttpError";
  readonly fatal = false;
  constructor(public readonly status: number) {
    super(`websocket upgrade failed with HTTP ${status}`);
  }
}

/** Server sent `closing` with reason `revoked`. Obtain a new token. Fatal. */
export class RevokedError extends WhdrError {
  override readonly name = "RevokedError";
  readonly fatal = true;
  constructor() {
    super("connection closed by server: token revoked");
  }
}

/** The application event handler threw. Fatal. */
export class HandlerError extends WhdrError {
  override readonly name = "HandlerError";
  readonly fatal = true;
  constructor(public override readonly cause: unknown) {
    super(`event handler failed: ${describe(cause)}`);
  }
}

/** A cursor-persistence hook failed. Fatal (cannot honour at-least-once). */
export class CursorStoreError extends WhdrError {
  override readonly name = "CursorStoreError";
  readonly fatal = true;
  constructor(public override readonly cause: unknown) {
    super(`cursor store failed: ${describe(cause)}`);
  }
}

/** The connection closed or the transport errored. Transient. */
export class ConnectionClosedError extends WhdrError {
  override readonly name = "ConnectionClosedError";
  readonly fatal = false;
  constructor(message = "connection closed", public override readonly cause?: unknown) {
    super(message);
  }
}

/** Building the connection request (URL/header) failed. Fatal. */
export class RequestError extends WhdrError {
  override readonly name = "RequestError";
  readonly fatal = true;
  constructor(message: string) {
    super(`invalid connection request: ${message}`);
  }
}

function describe(cause: unknown): string {
  if (cause instanceof Error) return cause.message;
  return String(cause);
}
