package whdrsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// ---------------------------------------------------------------------------
// In-process WebSocket test harness.
// ---------------------------------------------------------------------------

// srvConn is the server side of one accepted connection, exposing raw send and
// typed client-message receive to a script.
type srvConn struct {
	ws  *websocket.Conn
	ctx context.Context
}

// send writes a raw JSON text frame (ignores errors from an already-closed peer).
func (s *srvConn) send(raw string) {
	_ = s.ws.Write(s.ctx, websocket.MessageText, []byte(raw))
}

// sendEvent writes an event frame with the given id/seq on channel "t.echo".
func (s *srvConn) sendEvent(id string, seq uint64) {
	s.send(fmt.Sprintf(
		`{"type":"event","id":%q,"seq":%d,"ts_ms":1,"channel":"t.echo","payload_b64":"aGk="}`,
		id, seq))
}

// recvClient reads the next client message as a decoded map.
func (s *srvConn) recvClient() (map[string]any, error) {
	typ, data, err := s.ws.Read(s.ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageText {
		return map[string]any{}, nil
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// drain reads client frames until the connection errors (e.g. the client's
// close frame), letting a graceful client Close complete its handshake fast.
func (s *srvConn) drain() {
	for {
		if _, _, err := s.ws.Read(s.ctx); err != nil {
			return
		}
	}
}

// recvSubscribe reads a subscribe and returns its after_seq (and whether replay
// was present).
func (s *srvConn) recvSubscribe(t *testing.T) (afterSeq uint64, hasReplay bool) {
	t.Helper()
	m, err := s.recvClient()
	if err != nil {
		t.Fatalf("server: read subscribe: %v", err)
	}
	if m["type"] != "subscribe" {
		t.Fatalf("server: expected subscribe, got %v", m["type"])
	}
	if replay, ok := m["replay"].(map[string]any); ok {
		return uint64(replay["after_seq"].(float64)), true
	}
	return 0, false
}

type wsScript func(t *testing.T, connIdx int, sc *srvConn)

// startServer starts an httptest WebSocket server. Each accepted connection
// invokes script with a monotonically increasing connIdx. If expectToken is
// non-empty, an upgrade whose Authorization header does not match is rejected
// with HTTP 401 (before the WebSocket handshake).
func startServer(t *testing.T, expectToken string, script wsScript) string {
	t.Helper()
	var mu sync.Mutex
	connIdx := -1
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if expectToken != "" && r.Header.Get("Authorization") != "Bearer "+expectToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		ws.SetReadLimit(1 << 20)
		mu.Lock()
		connIdx++
		idx := connIdx
		mu.Unlock()
		script(t, idx, &srvConn{ws: ws, ctx: r.Context()})
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/subscribe"
}

// recorder is a test Handler that records everything it observes. It optionally
// runs afterEvent(seq) after handling an event (e.g. to cancel a context).
type recorder struct {
	mu          sync.Mutex
	events      []uint64
	gaps        [][2]uint64
	replayed    []uint64
	lagged      []uint64
	unavailable []string
	afterEvent  func(seq uint64)
	failOnSeq   uint64 // if non-zero, OnEvent returns an error for this seq
}

func (r *recorder) OnEvent(_ context.Context, ev EventFrame) error {
	r.mu.Lock()
	r.events = append(r.events, ev.Seq)
	hook := r.afterEvent
	fail := r.failOnSeq == ev.Seq
	r.mu.Unlock()
	if fail {
		return fmt.Errorf("handler forced failure at seq %d", ev.Seq)
	}
	if hook != nil {
		hook(ev.Seq)
	}
	return nil
}

func (r *recorder) OnReplayGap(_ context.Context, from, earliest uint64) error {
	r.mu.Lock()
	r.gaps = append(r.gaps, [2]uint64{from, earliest})
	r.mu.Unlock()
	return nil
}

func (r *recorder) OnReplayed(_ context.Context, through uint64) error {
	r.mu.Lock()
	r.replayed = append(r.replayed, through)
	r.mu.Unlock()
	return nil
}

func (r *recorder) OnLagged(_ context.Context, dropped uint64) error {
	r.mu.Lock()
	r.lagged = append(r.lagged, dropped)
	r.mu.Unlock()
	return nil
}

func (r *recorder) OnReplayUnavailable(_ context.Context, msg string) error {
	r.mu.Lock()
	r.unavailable = append(r.unavailable, msg)
	r.mu.Unlock()
	return nil
}

func (r *recorder) seqs() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.events...)
}

// fastBackoff keeps reconnect delays negligible in tests.
var fastBackoff = Backoff{Initial: time.Millisecond, Max: 5 * time.Millisecond, Multiplier: 2.0, Jitter: 0}

func equalU64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

// Subtlety #1 (ok before replay_gap) + subtlety #2 (earliest_seq delivered) +
// conformance items 4/7: dedup + explicit gap. The server sends ok FIRST, then
// replay_gap, then replayed events, then live — including a replay/live
// duplicate (by seq) and an id-duplicate — and the handler must see each event
// exactly once.
func TestRunReplayOrderingDedupAndGap(t *testing.T) {
	url := startServer(t, "tok", func(t *testing.T, idx int, sc *srvConn) {
		sc.send(`{"type":"welcome","name":"sub"}`)
		after, hasReplay := sc.recvSubscribe(t)
		if after != 0 || !hasReplay {
			t.Errorf("subscribe after_seq=%d hasReplay=%v, want 0/true", after, hasReplay)
		}
		// ok leads; replay_gap arrives AFTER ok (order-agnostic client).
		sc.send(`{"type":"ok","op":"subscribe"}`)
		sc.send(`{"type":"replay_gap","from_seq":0,"earliest_seq":3}`)
		// earliest_seq (3) itself is delivered.
		sc.sendEvent("e3", 3)
		sc.sendEvent("e4", 4)
		sc.send(`{"type":"replayed","through_seq":4}`)
		// Live: a duplicate of seq 4 (by seq guard) then a fresh seq 5.
		sc.sendEvent("e4", 4)
		sc.sendEvent("e5", 5)
		// id-duplicate: same id "e5" relabelled at a higher seq must be dropped.
		sc.sendEvent("e5", 99)
		sc.sendEvent("e6", 6)
		sc.send(`{"type":"closing","reason":"revoked"}`)
	})

	rec := &recorder{}
	err := Run(context.Background(), Config{URL: url, Token: "tok", Patterns: []string{"t.>"}, Backoff: fastBackoff}, rec)
	if !errors.Is(err, ErrRevoked) {
		t.Fatalf("Run err = %v, want ErrRevoked", err)
	}
	if got := rec.seqs(); !equalU64(got, []uint64{3, 4, 5, 6}) {
		t.Fatalf("processed seqs = %v, want [3 4 5 6] (exactly once)", got)
	}
	if len(rec.gaps) != 1 || rec.gaps[0] != [2]uint64{0, 3} {
		t.Fatalf("gaps = %v, want [[0 3]]", rec.gaps)
	}
	if len(rec.replayed) != 1 || rec.replayed[0] != 4 {
		t.Fatalf("replayed = %v, want [4]", rec.replayed)
	}
}

// Conformance item 6: lagged -> reconnect and resume from the cursor.
func TestRunLaggedReconnectsAndResumes(t *testing.T) {
	url := startServer(t, "tok", func(t *testing.T, idx int, sc *srvConn) {
		sc.send(`{"type":"welcome","name":"sub"}`)
		after, _ := sc.recvSubscribe(t)
		switch idx {
		case 0:
			if after != 0 {
				t.Errorf("first connect after_seq=%d, want 0", after)
			}
			sc.send(`{"type":"ok","op":"subscribe"}`)
			sc.sendEvent("e1", 1)
			sc.sendEvent("e2", 2)
			sc.send(`{"type":"lagged","dropped":5}`)
		default:
			// Resumed from the cursor (highest processed = 2).
			if after != 2 {
				t.Errorf("reconnect after_seq=%d, want 2", after)
			}
			sc.send(`{"type":"ok","op":"subscribe"}`)
			sc.sendEvent("e3", 3)
			sc.send(`{"type":"closing","reason":"revoked"}`)
		}
	})

	rec := &recorder{}
	err := Run(context.Background(), Config{URL: url, Token: "tok", Backoff: fastBackoff}, rec)
	if !errors.Is(err, ErrRevoked) {
		t.Fatalf("Run err = %v, want ErrRevoked", err)
	}
	if got := rec.seqs(); !equalU64(got, []uint64{1, 2, 3}) {
		t.Fatalf("processed = %v, want [1 2 3]", got)
	}
	if len(rec.lagged) != 1 || rec.lagged[0] != 5 {
		t.Fatalf("lagged = %v, want [5]", rec.lagged)
	}
}

// Conformance item 8: closing shutdown -> backoff reconnect (not fatal).
func TestRunShutdownReconnects(t *testing.T) {
	url := startServer(t, "tok", func(t *testing.T, idx int, sc *srvConn) {
		sc.send(`{"type":"welcome","name":"sub"}`)
		after, _ := sc.recvSubscribe(t)
		if idx == 0 {
			sc.send(`{"type":"ok","op":"subscribe"}`)
			sc.sendEvent("e1", 1)
			sc.send(`{"type":"closing","reason":"shutdown"}`)
		} else {
			if after != 1 {
				t.Errorf("reconnect after_seq=%d, want 1", after)
			}
			sc.sendEvent("e2", 2)
			sc.send(`{"type":"closing","reason":"revoked"}`)
		}
	})
	rec := &recorder{}
	err := Run(context.Background(), Config{URL: url, Token: "tok", Backoff: fastBackoff}, rec)
	if !errors.Is(err, ErrRevoked) {
		t.Fatalf("Run err = %v, want ErrRevoked", err)
	}
	if got := rec.seqs(); !equalU64(got, []uint64{1, 2}) {
		t.Fatalf("processed = %v, want [1 2]", got)
	}
}

// Conformance item 8: closing revoked -> fatal.
func TestRunRevokedIsFatal(t *testing.T) {
	url := startServer(t, "tok", func(t *testing.T, idx int, sc *srvConn) {
		if idx != 0 {
			t.Errorf("revoked should not reconnect; got connection %d", idx)
		}
		sc.send(`{"type":"welcome","name":"sub"}`)
		sc.recvSubscribe(t)
		sc.send(`{"type":"ok","op":"subscribe"}`)
		sc.send(`{"type":"closing","reason":"revoked"}`)
	})
	err := Run(context.Background(), Config{URL: url, Token: "tok", Backoff: fastBackoff}, &recorder{})
	if !errors.Is(err, ErrRevoked) {
		t.Fatalf("Run err = %v, want ErrRevoked", err)
	}
}

// Durability-disabled path: error op replay -> live-only continues.
func TestRunReplayUnavailableContinuesLive(t *testing.T) {
	url := startServer(t, "tok", func(t *testing.T, idx int, sc *srvConn) {
		sc.send(`{"type":"welcome","name":"sub"}`)
		_, hasReplay := sc.recvSubscribe(t)
		if !hasReplay {
			t.Error("expected a replay request on subscribe")
		}
		sc.send(`{"type":"ok","op":"subscribe"}`)
		sc.send(`{"type":"error","op":"replay","msg":"durable delivery disabled"}`)
		sc.sendEvent("e1", 1) // live delivery still works
		sc.send(`{"type":"closing","reason":"revoked"}`)
	})
	rec := &recorder{}
	err := Run(context.Background(), Config{URL: url, Token: "tok", Backoff: fastBackoff}, rec)
	if !errors.Is(err, ErrRevoked) {
		t.Fatalf("Run err = %v, want ErrRevoked", err)
	}
	if got := rec.seqs(); !equalU64(got, []uint64{1}) {
		t.Fatalf("processed = %v, want [1]", got)
	}
	if len(rec.unavailable) != 1 {
		t.Fatalf("unavailable callbacks = %v, want one", rec.unavailable)
	}
}

// A bad-pattern error (op subscribe) is non-fatal: the connection stays open.
func TestRunBadPatternErrorNonFatal(t *testing.T) {
	url := startServer(t, "tok", func(t *testing.T, idx int, sc *srvConn) {
		sc.send(`{"type":"welcome","name":"sub"}`)
		sc.recvSubscribe(t)
		sc.send(`{"type":"error","op":"subscribe","msg":"invalid pattern: !!"}`)
		sc.sendEvent("e1", 1)
		sc.send(`{"type":"closing","reason":"revoked"}`)
	})
	rec := &recorder{}
	err := Run(context.Background(), Config{URL: url, Token: "tok", Backoff: fastBackoff}, rec)
	if !errors.Is(err, ErrRevoked) {
		t.Fatalf("Run err = %v, want ErrRevoked", err)
	}
	if got := rec.seqs(); !equalU64(got, []uint64{1}) {
		t.Fatalf("processed = %v, want [1]", got)
	}
}

// Conformance items 1: bad token -> fatal ErrAuth (401), no reconnect.
func TestRunBadTokenIsFatalAuth(t *testing.T) {
	url := startServer(t, "goodtok", func(t *testing.T, idx int, sc *srvConn) {
		t.Errorf("bad token should never reach the WebSocket script (conn %d)", idx)
	})
	err := Run(context.Background(), Config{URL: url, Token: "wrongtok", Backoff: fastBackoff}, &recorder{})
	if !errors.Is(err, ErrAuth) {
		t.Fatalf("Run err = %v, want ErrAuth", err)
	}
	// Dial surfaces the same fatal error.
	_, derr := Dial(context.Background(), Config{URL: url, Token: "wrongtok"})
	if !errors.Is(derr, ErrAuth) {
		t.Fatalf("Dial err = %v, want ErrAuth", derr)
	}
}

// Conformance item 10 over the wire: unknown types and malformed frames are
// skipped without breaking the stream.
func TestRunToleratesUnknownAndMalformedFrames(t *testing.T) {
	url := startServer(t, "tok", func(t *testing.T, idx int, sc *srvConn) {
		sc.send(`{"type":"welcome","name":"sub"}`)
		sc.recvSubscribe(t)
		sc.send(`{"type":"ok","op":"subscribe"}`)
		sc.send(`{"type":"quantum_flux","x":1}`) // unknown type
		sc.send(`{"type":"event","id":"e1"`)     // malformed JSON
		sc.send(`{"type":"event","id":"e1","seq":1,"ts_ms":1,"channel":"t.echo","payload_b64":"aGk=","future":true}`)
		sc.send(`{"type":"closing","reason":"revoked"}`)
	})
	rec := &recorder{}
	err := Run(context.Background(), Config{URL: url, Token: "tok", Backoff: fastBackoff}, rec)
	if !errors.Is(err, ErrRevoked) {
		t.Fatalf("Run err = %v, want ErrRevoked", err)
	}
	if got := rec.seqs(); !equalU64(got, []uint64{1}) {
		t.Fatalf("processed = %v, want [1]", got)
	}
}

// Conformance item 5: a handler error is fatal and stops before advancing the
// cursor.
func TestRunHandlerErrorIsFatal(t *testing.T) {
	url := startServer(t, "tok", func(t *testing.T, idx int, sc *srvConn) {
		if idx != 0 {
			t.Errorf("handler error should not reconnect; got conn %d", idx)
		}
		sc.send(`{"type":"welcome","name":"sub"}`)
		sc.recvSubscribe(t)
		sc.send(`{"type":"ok","op":"subscribe"}`)
		sc.sendEvent("e1", 1)
		sc.sendEvent("e2", 2) // handler fails here
		sc.sendEvent("e3", 3)
		sc.send(`{"type":"closing","reason":"revoked"}`)
	})
	rec := &recorder{failOnSeq: 2}
	err := Run(context.Background(), Config{URL: url, Token: "tok", Backoff: fastBackoff}, rec)
	var he *HandlerError
	if !errors.As(err, &he) {
		t.Fatalf("Run err = %v, want *HandlerError", err)
	}
	if got := rec.seqs(); !equalU64(got, []uint64{1, 2}) {
		t.Fatalf("processed = %v, want [1 2] (stopped at failure)", got)
	}
}

// Cursor persistence: a CursorStore is loaded on start and saved after each
// event, enabling cross-restart resume.
func TestRunPersistsCursorToStore(t *testing.T) {
	url := startServer(t, "tok", func(t *testing.T, idx int, sc *srvConn) {
		sc.send(`{"type":"welcome","name":"sub"}`)
		after, _ := sc.recvSubscribe(t)
		if after != 10 {
			t.Errorf("after_seq=%d, want 10 (loaded from store)", after)
		}
		sc.send(`{"type":"ok","op":"subscribe"}`)
		sc.sendEvent("e11", 11)
		sc.send(`{"type":"closing","reason":"revoked"}`)
	})
	store := NewMemoryCursorStore(10)
	err := Run(context.Background(), Config{URL: url, Token: "tok", CursorStore: store, Backoff: fastBackoff}, &recorder{})
	if !errors.Is(err, ErrRevoked) {
		t.Fatalf("Run err = %v, want ErrRevoked", err)
	}
	if store.Get() != 11 {
		t.Fatalf("persisted cursor = %d, want 11", store.Get())
	}
}

// Context cancellation is respected everywhere: Run returns promptly with the
// context error when cancelled mid-stream.
func TestRunRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	url := startServer(t, "tok", func(t *testing.T, idx int, sc *srvConn) {
		sc.send(`{"type":"welcome","name":"sub"}`)
		sc.recvSubscribe(t)
		sc.send(`{"type":"ok","op":"subscribe"}`)
		sc.sendEvent("e1", 1) // handler cancels ctx after this
		<-sc.ctx.Done()       // then go silent until the client disconnects
	})
	rec := &recorder{afterEvent: func(seq uint64) { cancel() }}
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{URL: url, Token: "tok", Backoff: fastBackoff}, rec)
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

// Dial (bespoke path): typed Recv stream, welcome name, and application ping.
func TestDialTypedRecvAndPing(t *testing.T) {
	url := startServer(t, "tok", func(t *testing.T, idx int, sc *srvConn) {
		sc.send(`{"type":"welcome","name":"my-sub"}`)
		after, hasReplay := sc.recvSubscribe(t)
		if after != 7 || !hasReplay {
			t.Errorf("subscribe after_seq=%d hasReplay=%v, want 7/true", after, hasReplay)
		}
		sc.send(`{"type":"ok","op":"subscribe"}`)
		sc.sendEvent("e8", 8)
		// Expect an application ping, reply with pong.
		m, err := sc.recvClient()
		if err != nil {
			return
		}
		if m["type"] == "ping" {
			sc.send(`{"type":"pong"}`)
		}
		sc.drain()
	})

	conn, err := Dial(context.Background(), Config{URL: url, Token: "tok", Patterns: []string{"t.>"}, Cursor: 7})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	if conn.Name() != "my-sub" {
		t.Fatalf("name = %q, want my-sub", conn.Name())
	}
	ctx := context.Background()
	f, err := conn.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	ok, isOk := f.(OkFrame)
	if !isOk || ok.Op != "subscribe" {
		t.Fatalf("first frame = %#v, want OkFrame{subscribe}", f)
	}
	f, err = conn.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	ev, isEv := f.(EventFrame)
	if !isEv || ev.Seq != 8 {
		t.Fatalf("second frame = %#v, want EventFrame seq 8", f)
	}
	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	f, err = conn.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv pong: %v", err)
	}
	if _, isPong := f.(PongFrame); !isPong {
		t.Fatalf("frame = %#v, want PongFrame", f)
	}
}

// Conformance item 2: the client waits for welcome before subscribing; frames
// before welcome are skipped.
func TestDialSkipsFramesBeforeWelcome(t *testing.T) {
	url := startServer(t, "tok", func(t *testing.T, idx int, sc *srvConn) {
		// Junk before welcome — must be ignored by the client.
		sc.send(`{"type":"pong"}`)
		sc.send(`{"type":"quantum_flux"}`)
		sc.send(`{"type":"welcome","name":"late"}`)
		after, _ := sc.recvSubscribe(t) // proves welcome was consumed first
		_ = after
		sc.send(`{"type":"ok","op":"subscribe"}`)
		sc.drain()
	})
	conn, err := Dial(context.Background(), Config{URL: url, Token: "tok"})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	if conn.Name() != "late" {
		t.Fatalf("name = %q, want late", conn.Name())
	}
}
