package whdrsub

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/coder/websocket"
)

// defaultReadLimit bounds a single incoming frame. Ingest caps payloads at
// max_body_bytes (default 1 MiB); base64 inflates by ~4/3 plus JSON overhead,
// so 8 MiB leaves generous headroom. Override with Config.ReadLimit.
const defaultReadLimit = 8 << 20

// Conn is an authenticated subscriber connection, positioned just after the
// welcome frame (and, for a connection returned by Dial, after the subscribe
// has been sent). Recv yields typed Frames; the underlying WebSocket library
// answers protocol-level pings automatically (conformance item 9).
//
// A Conn is not safe for concurrent use by multiple goroutines. Recv from one
// goroutine at a time.
type Conn struct {
	ws     *websocket.Conn
	name   string
	cursor uint64
}

// Name is the subscriber name echoed in the welcome frame (the token's label).
func (c *Conn) Name() string { return c.name }

// Cursor is the resume cursor this connection subscribed with (0 if live-only).
func (c *Conn) Cursor() uint64 { return c.cursor }

// dial opens the WebSocket, authenticates, and consumes the welcome frame. It
// does not subscribe — Run and Dial layer their own subscribe on top.
func dial(ctx context.Context, cfg Config) (*Conn, error) {
	if cfg.URL == "" {
		return nil, &ConfigError{Msg: "empty URL"}
	}
	if cfg.Token == "" {
		return nil, &ConfigError{Msg: "empty token"}
	}
	header := http.Header{}
	header.Set("Authorization", "Bearer "+cfg.Token)
	ws, resp, err := websocket.Dial(ctx, cfg.URL, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		return nil, mapDialError(resp, err)
	}
	limit := cfg.ReadLimit
	if limit <= 0 {
		limit = defaultReadLimit
	}
	ws.SetReadLimit(limit)
	c := &Conn{ws: ws}
	// Read frames until the welcome; anything before it is skipped
	// (conformance item 2: wait for welcome before subscribing).
	for {
		frame, err := c.Recv(ctx)
		if err != nil {
			_ = c.ws.CloseNow()
			return nil, err
		}
		if w, ok := frame.(WelcomeFrame); ok {
			c.name = w.Name
			return c, nil
		}
		// A stray frame before welcome: ignore and keep reading.
	}
}

// Dial connects, authenticates, waits for welcome, and subscribes with the
// configured patterns and resume cursor. It returns a ready Conn whose Recv
// yields the typed frame stream. Use this for bespoke loops; most callers want
// Run, which additionally performs dedup, cursor advance, and reconnect.
//
// The resume cursor is taken from cfg.CursorStore.Load (if set) or cfg.Cursor.
// A subscribe carries replay.after_seq = cursor unless cfg.LiveOnly is set.
func Dial(ctx context.Context, cfg Config) (*Conn, error) {
	cursor, err := loadCursor(cfg)
	if err != nil {
		return nil, err
	}
	c, err := dial(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := c.Subscribe(ctx, cfg.Patterns, cursor, !cfg.LiveOnly); err != nil {
		_ = c.ws.CloseNow()
		return nil, err
	}
	c.cursor = cursor
	return c, nil
}

// Subscribe sends a subscribe. When replay is true the message carries
// replay.after_seq = cursor (conformance item 3: always resume from the
// cursor); otherwise it is a live-only subscription.
func (c *Conn) Subscribe(ctx context.Context, patterns []string, cursor uint64, replay bool) error {
	return c.send(ctx, subscribeMsg(patterns, cursor, replay))
}

// Unsubscribe removes patterns from this connection.
func (c *Conn) Unsubscribe(ctx context.Context, patterns []string) error {
	return c.send(ctx, clientMsg{Type: "unsubscribe", Patterns: patterns})
}

// Ping sends an application-level ping; the server replies with a PongFrame.
func (c *Conn) Ping(ctx context.Context) error {
	return c.send(ctx, clientMsg{Type: "ping"})
}

// send marshals and writes a client message as a text frame.
func (c *Conn) send(ctx context.Context, msg clientMsg) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return &ConfigError{Msg: "encoding client message: " + err.Error()}
	}
	if err := c.ws.Write(ctx, websocket.MessageText, data); err != nil {
		return mapTransportError(err)
	}
	return nil
}

// Recv reads the next typed server frame, skipping unrecognised frames
// (conformance item 10). Protocol-level pings are answered by the WebSocket
// library. Returns ErrConnClosed when the peer closes, or the context error if
// ctx is cancelled.
func (c *Conn) Recv(ctx context.Context) (Frame, error) {
	for {
		typ, data, err := c.ws.Read(ctx)
		if err != nil {
			return nil, mapTransportError(err)
		}
		if typ != websocket.MessageText {
			// Binary/other: ignore and keep reading.
			continue
		}
		frame, perr := parseFrame(data)
		if perr != nil {
			// Malformed JSON on a known type: skip it rather than tearing down
			// the connection (forward-compatibility posture).
			continue
		}
		if frame == nil {
			// Unknown "type": ignore (conformance item 10).
			continue
		}
		return frame, nil
	}
}

// Close closes the connection with a normal-closure status.
func (c *Conn) Close() error {
	return c.ws.Close(websocket.StatusNormalClosure, "")
}

// mapDialError translates a websocket.Dial failure. A 401 upgrade rejection
// maps to ErrAuth (fatal, conformance item 1); other HTTP statuses map to
// *HTTPError (transient); anything else is a transient connection error.
func mapDialError(resp *http.Response, err error) error {
	if resp != nil {
		if resp.StatusCode == http.StatusUnauthorized {
			return ErrAuth
		}
		if resp.StatusCode != http.StatusSwitchingProtocols {
			return &HTTPError{Status: resp.StatusCode}
		}
	}
	// No usable HTTP status (DNS, refused, TLS, timeout): transient.
	return errors.Join(ErrConnClosed, err)
}

// mapTransportError maps a read/write error. A context error is returned as-is
// so the Run loop can honour cancellation; everything else is ErrConnClosed
// (transient — reconnect and resume).
func mapTransportError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return errors.Join(ErrConnClosed, err)
}
