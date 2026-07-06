//go:build integration

// Integration tests against a real whdr-server binary. They are excluded from a
// plain `go test ./...` by the `integration` build tag, so the unit suite runs
// without the binaries.
//
// Run them with:
//
//	WHDR_SERVER_BIN=/path/to/target/debug/whdr-server \
//	WHDR_FAKE_EXT_BIN=/path/to/target/debug/examples/whdr-ext-fake \
//	go test -tags integration -run Integration ./...
//
// If either binary path is unset or missing, the tests skip with a note.
package whdrsub

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Harness: boot a real whdr-server with a scriptable fake echo extension.
// ---------------------------------------------------------------------------

type whdrServer struct {
	dir      string
	ingest   string // 127.0.0.1:port
	sub      string
	metrics  string
	ctlSock  string
	cmd      *exec.Cmd
	logBuf   *syncBuf
	extID    string
	delivery bool
}

type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}
func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func requireBinaries(t *testing.T) (serverBin, fakeBin string) {
	t.Helper()
	serverBin = os.Getenv("WHDR_SERVER_BIN")
	fakeBin = os.Getenv("WHDR_FAKE_EXT_BIN")
	if serverBin == "" || fakeBin == "" {
		t.Skip("set WHDR_SERVER_BIN and WHDR_FAKE_EXT_BIN to run integration tests")
	}
	for _, p := range []string{serverBin, fakeBin} {
		if _, err := os.Stat(p); err != nil {
			t.Skipf("binary %s not found: %v", p, err)
		}
	}
	return serverBin, fakeBin
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func copyFile(t *testing.T, src, dst string, mode os.FileMode) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, data, mode); err != nil {
		t.Fatal(err)
	}
}

// startWhdr boots a whdr-server with one fake echo extension (extID). When
// delivery is true, durable delivery is enabled.
func startWhdr(t *testing.T, extID string, delivery bool) *whdrServer {
	t.Helper()
	serverBin, fakeBin := requireBinaries(t)

	dir := t.TempDir()
	extsDir := filepath.Join(dir, "exts")
	if err := os.Mkdir(extsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Fake extension binary + (empty) behavior file = well-behaved echo.
	copyFile(t, fakeBin, filepath.Join(extsDir, "whdr-ext-"+extID), 0o755)
	if err := os.WriteFile(filepath.Join(extsDir, "whdr-ext-"+extID+".toml"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	// Secrets file (0600).
	if err := os.WriteFile(filepath.Join(dir, "secrets.toml"),
		[]byte(fmt.Sprintf("%s = \"secret-%s\"\n", extID, extID)), 0o600); err != nil {
		t.Fatal(err)
	}

	s := &whdrServer{
		dir:      dir,
		ingest:   freePort(t),
		sub:      freePort(t),
		metrics:  freePort(t),
		ctlSock:  filepath.Join(dir, "ctl.sock"),
		extID:    extID,
		delivery: delivery,
		logBuf:   &syncBuf{},
	}

	deliverySection := ""
	if delivery {
		deliverySection = fmt.Sprintf(
			"[delivery]\nenabled = true\nstore_path = \"%s\"\n\n",
			filepath.Join(dir, "delivery.redb"))
	}
	config := fmt.Sprintf(`[server]
listen_addr = "%s"
sub_addr = "%s"
metrics_addr = "%s"
control_socket = "%s"

[subscribers]
token_store = "%s"

[extensions]
enabled = ["%s"]

[limits]

[timeouts]

%s[secrets]
file = "%s"
`, s.ingest, s.sub, s.metrics, s.ctlSock,
		filepath.Join(dir, "tokens.toml"), extID, deliverySection,
		filepath.Join(dir, "secrets.toml"))
	configPath := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(configPath, []byte(config), 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(serverBin, "--config", configPath)
	cmd.Env = append(os.Environ(), "PATH="+extsDir+":"+os.Getenv("PATH"))
	cmd.Stdout = s.logBuf
	cmd.Stderr = s.logBuf
	if err := cmd.Start(); err != nil {
		t.Fatalf("start whdr-server: %v", err)
	}
	s.cmd = cmd
	t.Cleanup(func() {
		if s.cmd != nil && s.cmd.Process != nil {
			_ = s.cmd.Process.Kill()
			_, _ = s.cmd.Process.Wait()
		}
	})

	s.waitReady(t)
	s.waitExtRunning(t, extID)
	return s
}

func (s *whdrServer) subURL() string { return "ws://" + s.sub + "/subscribe" }

// control sends one control request and returns the decoded response.
func (s *whdrServer) control(t *testing.T, req map[string]any) map[string]any {
	t.Helper()
	conn, err := net.DialTimeout("unix", s.ctlSock, 2*time.Second)
	if err != nil {
		return nil
	}
	defer conn.Close()
	line, _ := json.Marshal(req)
	if _, err := conn.Write(append(line, '\n')); err != nil {
		return nil
	}
	r := bufio.NewReader(conn)
	respLine, err := r.ReadString('\n')
	if err != nil {
		return nil
	}
	var resp map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(respLine)), &resp); err != nil {
		return nil
	}
	return resp
}

func (s *whdrServer) waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if resp := s.control(t, map[string]any{"type": "status"}); resp["type"] == "status" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server not ready; log:\n%s", s.logBuf.String())
}

func (s *whdrServer) waitExtRunning(t *testing.T, id string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp := s.control(t, map[string]any{"type": "status"})
		if status, ok := resp["status"].(map[string]any); ok {
			if exts, ok := status["extensions"].([]any); ok {
				for _, e := range exts {
					if m, ok := e.(map[string]any); ok && m["id"] == id && m["state"] == "Ready" {
						return
					}
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("ext %s never reached Ready; log:\n%s", id, s.logBuf.String())
}

func (s *whdrServer) tokenAdd(t *testing.T, name string) string {
	t.Helper()
	resp := s.control(t, map[string]any{"type": "token.add", "name": name})
	tok, _ := resp["token"].(string)
	if tok == "" {
		t.Fatalf("token.add failed: %v", resp)
	}
	return tok
}

// emit POSTs a webhook to the extension route, which echoes it as an event on
// channel "<extID>.echo". Returns the assigned nothing (seq is server-global).
func (s *whdrServer) emit(t *testing.T, body string) {
	t.Helper()
	resp, err := http.Post("http://"+s.ingest+"/"+s.extID, "application/octet-stream",
		strings.NewReader(body))
	if err != nil {
		t.Fatalf("emit: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("emit status = %d, want 200 (log:\n%s)", resp.StatusCode, s.logBuf.String())
	}
}

// ---------------------------------------------------------------------------
// Test Handler.
// ---------------------------------------------------------------------------

type collector struct {
	mu          sync.Mutex
	seqs        []uint64
	ch          chan uint64
	unavailable bool
	gaps        int
}

func newCollector() *collector { return &collector{ch: make(chan uint64, 256)} }

func (c *collector) OnEvent(ctx context.Context, ev EventFrame) error {
	c.mu.Lock()
	c.seqs = append(c.seqs, ev.Seq)
	c.mu.Unlock()
	select {
	case c.ch <- ev.Seq:
	case <-ctx.Done():
	}
	return nil
}

func (c *collector) OnReplayUnavailable(_ context.Context, _ string) error {
	c.mu.Lock()
	c.unavailable = true
	c.mu.Unlock()
	return nil
}

func (c *collector) OnReplayGap(_ context.Context, _, _ uint64) error {
	c.mu.Lock()
	c.gaps++
	c.mu.Unlock()
	return nil
}

func (c *collector) allSeqs() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]uint64(nil), c.seqs...)
}

// waitSeq blocks until an event with seq target is observed or the deadline
// passes.
func (c *collector) waitSeq(t *testing.T, target uint64, within time.Duration) {
	t.Helper()
	deadline := time.After(within)
	for {
		select {
		case s := <-c.ch:
			if s == target {
				return
			}
		case <-deadline:
			t.Fatalf("did not observe seq %d within %v; saw %v", target, within, c.allSeqs())
		}
	}
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

// Live subscribe: a connected subscriber receives events as they are emitted.
func TestIntegrationLiveSubscribe(t *testing.T) {
	s := startWhdr(t, "alpha", true)
	token := s.tokenAdd(t, "watcher")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	col := newCollector()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{URL: s.subURL(), Token: token, Patterns: []string{">"}, Backoff: fastBackoff}, col)
	}()

	// Give the subscription a moment to establish before emitting.
	time.Sleep(200 * time.Millisecond)
	s.emit(t, "hello-live")
	col.waitSeq(t, 1, 5*time.Second)

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v, want context.Canceled", err)
	}
}

// Resume after disconnect replays missed events exactly-once at the handler.
func TestIntegrationResumeAfterDisconnectExactlyOnce(t *testing.T) {
	s := startWhdr(t, "alpha", true)
	token := s.tokenAdd(t, "resumer")
	store := NewFileCursorStore(filepath.Join(s.dir, "cursor"))

	// --- Session 1: connect, receive seq 1, persist the cursor, then stop.
	ctx1, cancel1 := context.WithCancel(context.Background())
	col1 := newCollector()
	done1 := make(chan error, 1)
	go func() {
		done1 <- Run(ctx1, Config{URL: s.subURL(), Token: token, Patterns: []string{">"},
			CursorStore: store, Backoff: fastBackoff}, col1)
	}()
	time.Sleep(200 * time.Millisecond)
	s.emit(t, "one")
	col1.waitSeq(t, 1, 5*time.Second)
	// Ensure the cursor was persisted before disconnecting.
	waitCursor(t, store, 1, 2*time.Second)
	cancel1()
	<-done1

	// --- While disconnected, emit two more events (seq 2, 3).
	s.emit(t, "two")
	s.emit(t, "three")

	// --- Session 2: resume from the persisted cursor. Must replay 2 and 3
	// exactly once (never re-deliver 1), then a live 4.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	col2 := newCollector()
	done2 := make(chan error, 1)
	go func() {
		done2 <- Run(ctx2, Config{URL: s.subURL(), Token: token, Patterns: []string{">"},
			CursorStore: store, Backoff: fastBackoff}, col2)
	}()
	col2.waitSeq(t, 3, 5*time.Second) // replayed
	time.Sleep(200 * time.Millisecond)
	s.emit(t, "four")
	col2.waitSeq(t, 4, 5*time.Second) // live

	cancel2()
	<-done2

	got := col2.allSeqs()
	for _, sq := range got {
		if sq == 1 {
			t.Fatalf("session 2 re-delivered seq 1 (already processed): %v", got)
		}
	}
	if !containsExactlyOnce(got, []uint64{2, 3, 4}) {
		t.Fatalf("session 2 seqs = %v, want 2,3,4 each exactly once", got)
	}
}

// Durability disabled: a replay request is refused (error op replay); the
// subscriber continues live-only.
func TestIntegrationDurabilityDisabledLiveOnly(t *testing.T) {
	s := startWhdr(t, "alpha", false) // no [delivery]
	token := s.tokenAdd(t, "live")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	col := newCollector()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Config{URL: s.subURL(), Token: token, Patterns: []string{">"},
			Cursor: 0, Backoff: fastBackoff}, col)
	}()
	time.Sleep(300 * time.Millisecond)
	s.emit(t, "live-only")
	col.waitSeq(t, 1, 5*time.Second)

	col.mu.Lock()
	unavailable := col.unavailable
	col.mu.Unlock()
	if !unavailable {
		t.Fatal("expected OnReplayUnavailable to fire when durability is disabled")
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run err = %v, want context.Canceled", err)
	}
	// No delivery store file when disabled.
	if _, err := os.Stat(filepath.Join(s.dir, "delivery.redb")); err == nil {
		t.Fatal("delivery store exists despite durability disabled")
	}
}

// Bad token is fatal (ErrAuth), no reconnect.
func TestIntegrationBadTokenFatal(t *testing.T) {
	s := startWhdr(t, "alpha", true)
	_ = s.tokenAdd(t, "real") // a valid token exists, but we use a bogus one.

	err := Run(context.Background(),
		Config{URL: s.subURL(), Token: "tok_bogus_not_a_real_token", Backoff: fastBackoff},
		newCollector())
	if !errors.Is(err, ErrAuth) {
		t.Fatalf("Run err = %v, want ErrAuth", err)
	}
}

// waitCursor polls a CursorStore until it reaches at least target.
func waitCursor(t *testing.T, store CursorStore, target uint64, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if c, _ := store.Load(); c >= target {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("cursor did not reach %d within %v", target, within)
}

func containsExactlyOnce(got, want []uint64) bool {
	counts := map[uint64]int{}
	for _, g := range got {
		counts[g]++
	}
	for _, w := range want {
		if counts[w] != 1 {
			return false
		}
	}
	return true
}
