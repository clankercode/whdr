package whdrsub

import (
	"context"
	"errors"
	"time"
)

// Config configures a subscriber. URL and Token are required; the rest have
// sensible defaults.
type Config struct {
	// URL is the /subscribe WebSocket endpoint, e.g.
	// "ws://127.0.0.1:8788/subscribe".
	URL string
	// Token is the tok_... bearer token minted by the operator.
	Token string
	// Patterns are the NATS-style channel patterns to subscribe to (e.g.
	// "github.>"). Empty means no patterns (the connection receives nothing
	// until it subscribes).
	Patterns []string

	// Cursor seeds the resume cursor (highest seq already processed) when
	// CursorStore is nil. 0 replays from the start of retention. Ignored if
	// CursorStore is set.
	Cursor uint64
	// CursorStore persists the cursor across sessions for at-least-once
	// delivery across restarts. If nil, an in-memory store seeded from Cursor
	// is used (no cross-restart durability).
	CursorStore CursorStore
	// DedupWindow is the number of recent event ids retained for replay/live
	// boundary dedup. Defaults to 8192 when <= 0.
	DedupWindow int

	// LiveOnly, when true, subscribes without a replay request (pre-v2
	// behaviour): live delivery only, no resume. Rarely wanted with Run.
	LiveOnly bool

	// Backoff overrides the reconnect schedule. Zero fields fall back to
	// DefaultBackoff.
	Backoff Backoff

	// ReadLimit bounds a single incoming frame in bytes. Defaults to 8 MiB.
	ReadLimit int64
}

const defaultDedupWindow = 8192

// Handler processes delivered events. Only OnEvent is required. To observe the
// out-of-band protocol signals, additionally implement any of the optional
// interfaces below (ReplayGapObserver, ReplayedObserver, LaggedObserver,
// ReplayUnavailableObserver); the Run loop type-asserts for them.
//
// Returning a non-nil error from OnEvent is fatal: Run stops and returns it
// wrapped in *HandlerError. The cursor is advanced (and persisted) only after
// OnEvent returns nil, giving at-least-once delivery.
type Handler interface {
	// OnEvent handles a delivered event. The Run loop has already de-duplicated
	// by id and seq, so this is called at most once per event. On nil, the
	// cursor advances to ev.Seq.
	OnEvent(ctx context.Context, ev EventFrame) error
}

// ReplayGapObserver is an optional Handler extension notified of an explicit,
// permanent data-loss signal: events in the open interval (fromSeq, earliestSeq)
// were pruned before this subscriber resumed (conformance item 7). Returning an
// error is fatal.
type ReplayGapObserver interface {
	OnReplayGap(ctx context.Context, fromSeq, earliestSeq uint64) error
}

// ReplayedObserver is an optional Handler extension notified when a replay
// window finished and live frames follow. throughSeq is the head the connection
// caught up to. Returning an error is fatal.
type ReplayedObserver interface {
	OnReplayed(ctx context.Context, throughSeq uint64) error
}

// LaggedObserver is an optional Handler extension notified when the server
// evicted dropped events for this connection. The Run loop then reconnects and
// replays from the cursor to recover. Returning an error is fatal.
type LaggedObserver interface {
	OnLagged(ctx context.Context, dropped uint64) error
}

// ReplayUnavailableObserver is an optional Handler extension notified when a
// replay request was refused because durable delivery is disabled on the server
// (error op "replay"); live delivery still works. Returning an error is fatal.
type ReplayUnavailableObserver interface {
	OnReplayUnavailable(ctx context.Context, msg string) error
}

// loadCursor resolves the starting cursor: from the CursorStore if set, else
// from Config.Cursor.
func loadCursor(cfg Config) (uint64, error) {
	if cfg.CursorStore != nil {
		c, err := cfg.CursorStore.Load()
		if err != nil {
			return 0, &CursorStoreError{Err: err}
		}
		return c, nil
	}
	return cfg.Cursor, nil
}

// saveCursor persists the cursor if a store is configured.
func saveCursor(cfg Config, cursor uint64) error {
	if cfg.CursorStore == nil {
		return nil
	}
	if err := cfg.CursorStore.Save(cursor); err != nil {
		return &CursorStoreError{Err: err}
	}
	return nil
}

// Run drives the full reconnect-and-resume loop (appendix §7), calling handler
// for each event.
//
// It loops forever, reconnecting with exponential backoff after a transient
// failure (dropped socket, server shutdown, lagged eviction). It returns only
// on a fatal error — a revoked/absent token (ErrAuth / ErrRevoked), a handler
// failure (*HandlerError), a cursor-store failure (*CursorStoreError), a config
// error (*ConfigError) — or when ctx is cancelled (returns ctx.Err()).
func Run(ctx context.Context, cfg Config, handler Handler) error {
	if handler == nil {
		return &ConfigError{Msg: "nil handler"}
	}
	cursor, err := loadCursor(cfg)
	if err != nil {
		return err
	}
	window := cfg.DedupWindow
	if window <= 0 {
		window = defaultDedupWindow
	}
	resume := newResumeState(cursor, window)
	backoff := cfg.Backoff.start()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := runSession(ctx, cfg, handler, resume, backoff)
		switch {
		case err == nil:
			// Clean end (shutdown / lagged): reconnect and resume.
		case errors.Is(err, errReconnect):
			// Transient signal: reconnect and resume.
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return err
		case isFatal(err):
			return err
		default:
			// Transient transport error: reconnect with backoff.
		}
		if err := sleepWithContext(ctx, backoff.nextDelay()); err != nil {
			return err
		}
	}
}

// runSession runs one connection's lifetime. A nil return (or errReconnect)
// means "reconnect and resume"; a fatal error stops the loop.
func runSession(ctx context.Context, cfg Config, handler Handler, resume *resumeState, backoff *backoffRunner) error {
	c, err := dial(ctx, cfg)
	if err != nil {
		return err
	}
	defer c.ws.CloseNow()

	// Connected: reset backoff so a later drop reconnects fast.
	backoff.reset()
	if err := c.Subscribe(ctx, cfg.Patterns, resume.Cursor(), !cfg.LiveOnly); err != nil {
		return err
	}

	for {
		frame, err := c.Recv(ctx)
		if err != nil {
			return err
		}
		switch f := frame.(type) {
		case EventFrame:
			if !resume.shouldProcess(f.ID, f.Seq) {
				continue // duplicate (replay/live overlap) — ignore
			}
			if herr := handler.OnEvent(ctx, f); herr != nil {
				if errors.Is(herr, context.Canceled) || errors.Is(herr, context.DeadlineExceeded) {
					return herr
				}
				return &HandlerError{Err: herr}
			}
			resume.record(f.ID, f.Seq)
			if serr := saveCursor(cfg, resume.Cursor()); serr != nil {
				return serr
			}
		case ReplayedFrame:
			if obs, ok := handler.(ReplayedObserver); ok {
				if herr := obs.OnReplayed(ctx, f.ThroughSeq); herr != nil {
					return handlerHookError(herr)
				}
			}
		case ReplayGapFrame:
			if obs, ok := handler.(ReplayGapObserver); ok {
				if herr := obs.OnReplayGap(ctx, f.FromSeq, f.EarliestSeq); herr != nil {
					return handlerHookError(herr)
				}
			}
		case LaggedFrame:
			if obs, ok := handler.(LaggedObserver); ok {
				if herr := obs.OnLagged(ctx, f.Dropped); herr != nil {
					return handlerHookError(herr)
				}
			}
			// Recover by reconnecting and replaying from the cursor.
			return errReconnect
		case ErrorFrame:
			if f.Op == "replay" {
				// Durability disabled: live-only continues on this connection.
				if obs, ok := handler.(ReplayUnavailableObserver); ok {
					if herr := obs.OnReplayUnavailable(ctx, f.Msg); herr != nil {
						return handlerHookError(herr)
					}
				}
			}
			// op == "subscribe" (bad pattern) or other: non-fatal; keep going.
		case ClosingFrame:
			if f.Reason == ReasonRevoked {
				return ErrRevoked
			}
			// shutdown: reconnect with backoff.
			return errReconnect
		case WelcomeFrame, OkFrame, PongFrame:
			// Nothing to do.
		}
	}
}

// handlerHookError classifies an error returned by an optional Handler hook:
// a context error passes through; anything else is fatal (*HandlerError).
func handlerHookError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return &HandlerError{Err: err}
}

// sleepWithContext sleeps for d, returning early with ctx.Err() if ctx is
// cancelled first.
func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
