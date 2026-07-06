"""Frame parsing, unknown-type tolerance, and subscribe serialisation."""

from __future__ import annotations

import base64
import json

from whdr_sub_kit import (
    Closing,
    DeliveredEvent,
    ErrorFrame,
    Lagged,
    Ok,
    Pong,
    Replayed,
    ReplayGap,
    Welcome,
    parse_frame,
    subscribe_message,
)


def test_parses_event_frame() -> None:
    text = json.dumps(
        {
            "type": "event",
            "id": "00000000-0000-0000-0000-000000000000",
            "seq": 7,
            "ts_ms": 1751760000000,
            "channel": "github.push",
            "payload_b64": "AA==",
        }
    )
    frame = parse_frame(text)
    assert isinstance(frame, DeliveredEvent)
    assert frame.seq == 7
    assert frame.channel == "github.push"
    assert frame.payload() == b"\x00"


def test_parses_all_control_frames() -> None:
    assert parse_frame('{"type":"welcome","name":"p"}') == Welcome("p")
    assert parse_frame('{"type":"ok","op":"subscribe"}') == Ok("subscribe")
    assert parse_frame('{"type":"error","op":"replay","msg":"x"}') == ErrorFrame("replay", "x")
    assert parse_frame('{"type":"replayed","through_seq":2}') == Replayed(2)
    assert parse_frame('{"type":"replay_gap","from_seq":1,"earliest_seq":5}') == ReplayGap(1, 5)
    assert parse_frame('{"type":"lagged","dropped":42}') == Lagged(42)
    assert parse_frame('{"type":"pong"}') == Pong()
    assert parse_frame('{"type":"closing","reason":"shutdown"}') == Closing("shutdown")
    assert parse_frame('{"type":"closing","reason":"revoked"}') == Closing("revoked")


def test_skips_unknown_frame_type() -> None:
    # Conformance item 10: unknown `type` values are ignored.
    assert parse_frame('{"type":"quantum_flux","foo":1}') is None
    assert parse_frame("not json at all") is None
    assert parse_frame('["not","an","object"]') is None
    assert parse_frame('{"no":"type field"}') is None


def test_tolerates_unknown_fields_on_known_frame() -> None:
    # Conformance item 10: unknown object fields are ignored.
    frame = parse_frame('{"type":"welcome","name":"p","future_field":{"nested":true}}')
    assert frame == Welcome("p")
    # Extra field on an event does not break parsing.
    ev = parse_frame(
        '{"type":"event","id":"x","seq":1,"ts_ms":0,"channel":"a.b",'
        '"payload_b64":"AA==","surprise":9}'
    )
    assert isinstance(ev, DeliveredEvent)
    assert ev.channel == "a.b"


def test_rejects_malformed_known_frame() -> None:
    # A known type missing a required field is ignored (returns None), not raised.
    assert parse_frame('{"type":"event","id":"x"}') is None
    # Wrong field type (seq as string) is ignored.
    assert parse_frame(
        '{"type":"event","id":"x","seq":"nope","ts_ms":0,"channel":"a","payload_b64":"AA=="}'
    ) is None
    # A JSON bool must not be accepted as the integer seq.
    assert parse_frame(
        '{"type":"event","id":"x","seq":true,"ts_ms":0,"channel":"a","payload_b64":"AA=="}'
    ) is None


def test_parses_bytes_frame() -> None:
    assert parse_frame(b'{"type":"pong"}') == Pong()
    # Invalid UTF-8 bytes are ignored.
    assert parse_frame(b"\xff\xfe") is None


def test_delivered_event_decodes_payload() -> None:
    ev = DeliveredEvent(
        id="id", seq=1, ts_ms=0, channel="dev.x", payload_b64=base64.standard_b64encode(b"hello").decode()
    )
    assert ev.payload() == b"hello"


def test_subscribe_message_with_and_without_cursor() -> None:
    # Conformance item 3: resume with replay.after_seq = cursor.
    with_cursor = json.loads(subscribe_message(["github.>"], 128))
    assert with_cursor == {
        "type": "subscribe",
        "patterns": ["github.>"],
        "replay": {"after_seq": 128},
    }
    # after_seq = 0 still attaches a replay cursor (0 = from retention start).
    zero = json.loads(subscribe_message(["a.>"], 0))
    assert zero["replay"] == {"after_seq": 0}
    # None => live-only, no replay key.
    live = json.loads(subscribe_message(["a.>"], None))
    assert "replay" not in live
