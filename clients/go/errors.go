package whdrsub

import (
	"errors"
	"fmt"
)

// Sentinel errors surfaced by the client. Fatal errors stop the Run loop and
// are returned; transient errors cause a backoff reconnect.
var (
	// ErrAuth means the WebSocket upgrade was rejected with HTTP 401: the token
	// is missing, wrong, or revoked. Fatal (conformance item 1).
	ErrAuth = errors.New("whdrsub: authentication failed (HTTP 401): token missing, wrong, or revoked")

	// ErrRevoked means the server sent closing with reason "revoked": the token
	// was rotated or revoked mid-connection. Obtain a new token. Fatal.
	ErrRevoked = errors.New("whdrsub: connection closed by server: token revoked")

	// ErrConnClosed means the connection closed (cleanly or with a close frame
	// carrying no actionable reason). Transient — reconnect and resume.
	ErrConnClosed = errors.New("whdrsub: connection closed")

	// errReconnect is an internal signal: end this session and reconnect with a
	// resume from the cursor (lagged eviction, server shutdown). Transient.
	errReconnect = errors.New("whdrsub: reconnect and resume")
)

// HTTPError is a non-401 HTTP status on the WebSocket upgrade. Transient by
// default (the server may be starting up or briefly unavailable).
type HTTPError struct {
	Status int
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("whdrsub: websocket upgrade failed with HTTP %d", e.Status)
}

// HandlerError wraps an error returned by the application event handler. Fatal:
// the Run loop stops so the caller sees the failure rather than silently
// looping. The cursor is only advanced after the handler succeeds.
type HandlerError struct {
	Err error
}

func (e *HandlerError) Error() string { return "whdrsub: event handler failed: " + e.Err.Error() }
func (e *HandlerError) Unwrap() error { return e.Err }

// CursorStoreError wraps a failure from the cursor persistence hook. Fatal: a
// client that cannot persist its cursor cannot honour at-least-once delivery.
type CursorStoreError struct {
	Err error
}

func (e *CursorStoreError) Error() string { return "whdrsub: cursor store failed: " + e.Err.Error() }
func (e *CursorStoreError) Unwrap() error { return e.Err }

// ConfigError means the configuration or connection request was invalid (bad
// URL, empty token, malformed header). Fatal — retrying will not help.
type ConfigError struct {
	Msg string
}

func (e *ConfigError) Error() string { return "whdrsub: invalid configuration: " + e.Msg }

// isFatal reports whether the Run loop should stop and return err rather than
// reconnecting. context.Canceled / context.DeadlineExceeded are handled
// separately by the loop (they end the loop with the context error).
func isFatal(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrAuth) || errors.Is(err, ErrRevoked) {
		return true
	}
	var (
		handlerErr *HandlerError
		cursorErr  *CursorStoreError
		configErr  *ConfigError
	)
	return errors.As(err, &handlerErr) ||
		errors.As(err, &cursorErr) ||
		errors.As(err, &configErr)
}
