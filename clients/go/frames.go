package whdrsub

import (
	"encoding/base64"
	"encoding/json"
)

// Frame is the sealed set of server -> client messages in the Subscriber wire
// protocol v2. Every concrete frame type implements it. Callers switch on the
// dynamic type. Unknown frame "type" tags and unknown object fields are
// tolerated (conformance item 10): unknown types decode to nil (skipped by the
// connection layer) and unknown fields are ignored by encoding/json's default
// behaviour.
type Frame interface {
	isFrame()
}

// WelcomeFrame is the first frame after a successful auth. It echoes the
// subscriber name (the token's label).
type WelcomeFrame struct {
	Name string `json:"name"`
}

// OkFrame acknowledges a subscribe/unsubscribe. Op is the client op name.
type OkFrame struct {
	Op string `json:"op"`
}

// ErrorFrame reports that an op failed. It is non-fatal; the connection stays
// open. Only Op == "replay" (durability disabled) is contractual; never match
// on Msg text.
type ErrorFrame struct {
	Op  string `json:"op"`
	Msg string `json:"msg"`
}

// EventFrame carries a delivered event. The same ID/Seq are used whether the
// event is delivered live or via replay — dedup by ID.
type EventFrame struct {
	// ID is the stable event identity (a UUID string). Dedup key.
	ID string `json:"id"`
	// Seq is the global monotonic sequence (cursor key). Gaps are normal.
	Seq uint64 `json:"seq"`
	// TsMs is the server wall-clock at fan-out (unix ms); informational only.
	TsMs uint64 `json:"ts_ms"`
	// Channel is the channel the event was published on.
	Channel string `json:"channel"`
	// PayloadB64 is standard base64 of the raw event bytes.
	PayloadB64 string `json:"payload_b64"`
}

// Payload decodes PayloadB64 to the raw event bytes.
func (e EventFrame) Payload() ([]byte, error) {
	return base64.StdEncoding.DecodeString(e.PayloadB64)
}

// ReplayedFrame is sent after a replay window is fully delivered; live frames
// follow. ThroughSeq is the head the connection caught up to.
type ReplayedFrame struct {
	ThroughSeq uint64 `json:"through_seq"`
}

// ReplayGapFrame is an explicit, permanent data-loss signal: events in the open
// interval (FromSeq, EarliestSeq) were pruned before this subscriber resumed.
// Note EarliestSeq itself IS delivered — only strictly-interior events are gone.
type ReplayGapFrame struct {
	FromSeq     uint64 `json:"from_seq"`
	EarliestSeq uint64 `json:"earliest_seq"`
}

// LaggedFrame means the outbound queue overflowed and the server evicted
// Dropped events for this connection. Recover by reconnecting and replaying
// from the cursor.
type LaggedFrame struct {
	Dropped uint64 `json:"dropped"`
}

// PongFrame is the reply to a client {"type":"ping"}.
type PongFrame struct{}

// ClosingReason is the reason carried by a ClosingFrame.
type ClosingReason string

const (
	// ReasonShutdown means the server is shutting down; reconnect with backoff.
	ReasonShutdown ClosingReason = "shutdown"
	// ReasonRevoked means the token was rotated/revoked; obtain a new one. Fatal.
	ReasonRevoked ClosingReason = "revoked"
)

// ClosingFrame is sent when the server closes this connection.
type ClosingFrame struct {
	Reason ClosingReason `json:"reason"`
}

func (WelcomeFrame) isFrame()   {}
func (OkFrame) isFrame()        {}
func (ErrorFrame) isFrame()     {}
func (EventFrame) isFrame()     {}
func (ReplayedFrame) isFrame()  {}
func (ReplayGapFrame) isFrame() {}
func (LaggedFrame) isFrame()    {}
func (PongFrame) isFrame()      {}
func (ClosingFrame) isFrame()   {}

// parseFrame decodes one JSON text frame into a typed Frame.
//
// It returns (nil, nil) for unknown "type" tags and for otherwise-undecodable
// frames (conformance item 10 — forward compatibility): the caller ignores the
// frame and reads the next one. Unknown *fields* on a known frame are tolerated
// by encoding/json's default behaviour. A genuine JSON syntax error on a known
// type is returned so it is not silently swallowed.
func parseFrame(text []byte) (Frame, error) {
	var envelope struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(text, &envelope); err != nil {
		// Not even a JSON object with a type tag: ignore (forward compat).
		return nil, nil
	}
	switch envelope.Type {
	case "welcome":
		var f WelcomeFrame
		return decodeInto(text, &f)
	case "ok":
		var f OkFrame
		return decodeInto(text, &f)
	case "error":
		var f ErrorFrame
		return decodeInto(text, &f)
	case "event":
		var f EventFrame
		return decodeInto(text, &f)
	case "replayed":
		var f ReplayedFrame
		return decodeInto(text, &f)
	case "replay_gap":
		var f ReplayGapFrame
		return decodeInto(text, &f)
	case "lagged":
		var f LaggedFrame
		return decodeInto(text, &f)
	case "pong":
		return PongFrame{}, nil
	case "closing":
		var f ClosingFrame
		return decodeInto(text, &f)
	default:
		// Unknown type: ignore (conformance item 10).
		return nil, nil
	}
}

// decodeInto unmarshals text into f and returns it as a Frame. f must be a
// pointer to a concrete frame type that implements Frame.
func decodeInto[T Frame](text []byte, f *T) (Frame, error) {
	if err := json.Unmarshal(text, f); err != nil {
		return nil, err
	}
	return *f, nil
}

// clientMsg is the client -> server envelope, marshalled to JSON text frames.
type clientMsg struct {
	Type     string         `json:"type"`
	Patterns []string       `json:"patterns,omitempty"`
	Replay   *replayRequest `json:"replay,omitempty"`
}

// replayRequest is the optional resume cursor on a subscribe. AfterSeq is
// exclusive: the server streams stored events with seq > after_seq.
type replayRequest struct {
	AfterSeq uint64 `json:"after_seq"`
}

// subscribeMsg builds a subscribe message. When replay is true a resume cursor
// is attached (AfterSeq = cursor); otherwise the subscription is live-only.
func subscribeMsg(patterns []string, cursor uint64, replay bool) clientMsg {
	msg := clientMsg{Type: "subscribe", Patterns: patterns}
	if replay {
		msg.Replay = &replayRequest{AfterSeq: cursor}
	}
	return msg
}
