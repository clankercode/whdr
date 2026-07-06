package whdrsub

import (
	"testing"
)

func TestParseKnownEventFrame(t *testing.T) {
	text := []byte(`{"type":"event","id":"00000000-0000-0000-0000-000000000000",
		"seq":7,"ts_ms":1751760000000,"channel":"github.push","payload_b64":"aGVsbG8="}`)
	frame, err := parseFrame(text)
	if err != nil {
		t.Fatalf("parseFrame: %v", err)
	}
	ev, ok := frame.(EventFrame)
	if !ok {
		t.Fatalf("expected EventFrame, got %T", frame)
	}
	if ev.Seq != 7 || ev.Channel != "github.push" {
		t.Fatalf("bad event: %+v", ev)
	}
	body, err := ev.Payload()
	if err != nil {
		t.Fatalf("Payload: %v", err)
	}
	if string(body) != "hello" {
		t.Fatalf("payload = %q, want hello", body)
	}
}

// Conformance item 10: unknown "type" values are ignored (nil, nil).
func TestSkipsUnknownFrameType(t *testing.T) {
	for _, text := range []string{
		`{"type":"quantum_flux","foo":1}`,
		`not json at all`,
		`{"no_type":true}`,
	} {
		frame, err := parseFrame([]byte(text))
		if err != nil {
			t.Fatalf("parseFrame(%q) err = %v", text, err)
		}
		if frame != nil {
			t.Fatalf("parseFrame(%q) = %v, want nil", text, frame)
		}
	}
}

// Conformance item 10: unknown object fields on a known frame are ignored.
func TestToleratesUnknownFieldsOnKnownFrame(t *testing.T) {
	text := []byte(`{"type":"welcome","name":"p","future_field":{"nested":true}}`)
	frame, err := parseFrame(text)
	if err != nil {
		t.Fatalf("parseFrame: %v", err)
	}
	w, ok := frame.(WelcomeFrame)
	if !ok {
		t.Fatalf("expected WelcomeFrame, got %T", frame)
	}
	if w.Name != "p" {
		t.Fatalf("name = %q, want p", w.Name)
	}
}

func TestParseAllServerFrameTypes(t *testing.T) {
	cases := []struct {
		text string
		want Frame
	}{
		{`{"type":"welcome","name":"sub-a"}`, WelcomeFrame{Name: "sub-a"}},
		{`{"type":"ok","op":"subscribe"}`, OkFrame{Op: "subscribe"}},
		{`{"type":"error","op":"replay","msg":"durability disabled"}`, ErrorFrame{Op: "replay", Msg: "durability disabled"}},
		{`{"type":"replayed","through_seq":42}`, ReplayedFrame{ThroughSeq: 42}},
		{`{"type":"replay_gap","from_seq":1,"earliest_seq":5}`, ReplayGapFrame{FromSeq: 1, EarliestSeq: 5}},
		{`{"type":"lagged","dropped":9}`, LaggedFrame{Dropped: 9}},
		{`{"type":"pong"}`, PongFrame{}},
		{`{"type":"closing","reason":"shutdown"}`, ClosingFrame{Reason: ReasonShutdown}},
		{`{"type":"closing","reason":"revoked"}`, ClosingFrame{Reason: ReasonRevoked}},
	}
	for _, tc := range cases {
		frame, err := parseFrame([]byte(tc.text))
		if err != nil {
			t.Fatalf("parseFrame(%q): %v", tc.text, err)
		}
		if frame != tc.want {
			t.Fatalf("parseFrame(%q) = %#v, want %#v", tc.text, frame, tc.want)
		}
	}
}

// Conformance item 3: subscribe carries replay.after_seq = cursor.
func TestSubscribeMsgUsesCursorAsAfterSeq(t *testing.T) {
	msg := subscribeMsg([]string{"github.>"}, 128, true)
	if msg.Type != "subscribe" || len(msg.Patterns) != 1 || msg.Patterns[0] != "github.>" {
		t.Fatalf("bad subscribe: %+v", msg)
	}
	if msg.Replay == nil || msg.Replay.AfterSeq != 128 {
		t.Fatalf("replay = %+v, want after_seq=128", msg.Replay)
	}
	// Live-only when replay is false.
	live := subscribeMsg([]string{"github.>"}, 128, false)
	if live.Replay != nil {
		t.Fatalf("live-only subscribe should omit replay, got %+v", live.Replay)
	}
}
