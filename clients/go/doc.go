// Package whdrsub is the Go client library for the whdr subscriber plane and an
// implementation of the Subscriber wire protocol v2 (durable delivery /
// replay). It mirrors the behaviour of the reference Rust crate whdr-sub-kit.
//
// whdr fans provider-webhook events out to token-authenticated WebSocket
// subscribers. With durable delivery enabled on the server, a subscriber can
// resume from a cursor and replay events it missed while offline or after a
// slow-consumer drop — at-least-once, deduplicated by event id.
//
// # Two ways to use it
//
//   - Run — the batteries-included loop. Implement Handler, hand it to Run, and
//     the library performs the full reconnect-and-resume algorithm for you:
//     auth, welcome, subscribe with replay.after_seq = cursor, dedup by id and
//     seq, advance the cursor after each successful handle, recover from lagged
//     and disconnects by reconnecting from the cursor, surface replay_gap, treat
//     revoked as fatal and shutdown as a backoff reconnect. This is what most
//     callers want.
//   - Dial — the typed frame stream. Get a Conn and drive Conn.Recv yourself.
//     Use this when you need bespoke control over the loop.
//
// # Example
//
//	type printer struct{}
//
//	func (printer) OnEvent(ctx context.Context, ev whdrsub.EventFrame) error {
//		body, err := ev.Payload()
//		if err != nil {
//			return err
//		}
//		log.Printf("seq=%d channel=%s %d bytes", ev.Seq, ev.Channel, len(body))
//		return nil
//	}
//
//	func main() {
//		cfg := whdrsub.Config{
//			URL:      "ws://127.0.0.1:8788/subscribe",
//			Token:    os.Getenv("WHDR_TOKEN"),
//			Patterns: []string{"github.>"},
//			Cursor:   0, // 0 = replay from the start of retention
//		}
//		// Runs until ctx is cancelled or a fatal error (revoked token, auth
//		// failure, handler error).
//		if err := whdrsub.Run(context.Background(), cfg, printer{}); err != nil {
//			log.Fatal(err)
//		}
//	}
//
// # Conformance
//
// This library implements the 10-point client-library conformance checklist
// from the Subscriber wire protocol v2 appendix.
package whdrsub
