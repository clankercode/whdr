# whdr-sub-kit (Python)

Async Python client for the **whdr** subscriber plane and an implementation of
the *Subscriber wire protocol v2* (durable delivery / replay). Behaviour mirrors
the reference Rust `whdr-sub-kit`; see the repo's `docs/SUBSCRIBERS.md` and
SPEC §9 / §9.4.

- Python ≥ 3.11, asyncio. Runtime dependency: `websockets` only.
- Fully typed (`py.typed`, mypy-strict clean).

## Install (from a checkout, with `uv`)

```bash
cd clients/python
uv sync --extra dev      # create the venv and install runtime + dev deps
uv run pytest            # run the test suite
```

## Two ways to consume events

### 1. The event iterator

```python
import asyncio
from whdr_sub_kit import Subscriber

async def main() -> None:
    sub = Subscriber(
        "ws://127.0.0.1:8788/subscribe",
        "tok_your_token",
        patterns=["github.>"],   # NATS-style; str or list of str
        cursor=0,                # 0 = replay from the start of retention
    )
    async for event in sub.events():
        body = event.payload()   # base64-decoded bytes
        print(f"seq={event.seq} channel={event.channel} {len(body)} bytes")

asyncio.run(main())
```

The cursor advances (and is persisted, if you install a `CursorStore`) once your
loop body finishes each event — at-least-once delivery. `replay_gap` / `lagged`
are surfaced via optional constructor callbacks (`on_replay_gap`, `on_lagged`,
`on_replayed`, `on_replay_unavailable`); `replay_gap` logs a warning by default.

### 2. The `run(handler)` loop

```python
from whdr_sub_kit import Subscriber, Handler, DeliveredEvent

class Printer(Handler):
    async def on_event(self, event: DeliveredEvent) -> None:
        print(event.seq, event.channel)

    async def on_replay_gap(self, from_seq: int, earliest_seq: int) -> None:
        print(f"data loss: ({from_seq}, {earliest_seq}) were pruned")

sub = Subscriber("ws://127.0.0.1:8788/subscribe", "tok_...", patterns=["github.>"])
await sub.run(Printer())      # loops forever; raises only on a fatal error
```

`run` also accepts a bare async callable `async def handler(event): ...`.

## What the reconnect-and-resume loop does (appendix §7)

- Authenticates with `Authorization: Bearer <token>`; HTTP 401 is fatal.
- Waits for `welcome`, then subscribes with `replay.after_seq = cursor` on every
  (re)connect.
- De-duplicates by event `id` and skips `seq <= cursor`, so each event is
  handled at most once across the replay/live boundary.
- Advances the cursor only after your handler / loop body succeeds.
- Recovers from `lagged` and dropped sockets by reconnecting and replaying from
  the cursor; surfaces `replay_gap` (permanent, pruned loss).
- Treats `closing` `revoked` as fatal (renew your token) and `shutdown` as a
  backoff reconnect. Backoff is exponential with jitter.
- Ignores unknown frame `type`s and unknown fields (forward compatibility).

## Persisting the cursor across restarts

Implement the `CursorStore` protocol (async `load` / `save`) and pass it as
`cursor_store=...`. `save` is called after each successfully-handled event;
`load` supplies the starting cursor. Without one, an in-memory cursor is used.

## Conformance map (protocol appendix §9 checklist → test)

| # | Requirement | Test(s) |
|---|-------------|---------|
| 1 | `Authorization: Bearer` on upgrade; 401 fatal | `test_mock_server.py::test_bad_token_is_fatal_auth_error`, `test_integration.py::test_bad_token_is_fatal` |
| 2 | Wait for `welcome` before subscribing | `test_mock_server.py::test_replay_then_live_stream` (server reads `subscribe` only after welcome handshake) |
| 3 | `subscribe` with `replay.after_seq = cursor` every (re)connect | `test_frames.py::test_subscribe_message_with_and_without_cursor`, `test_mock_server.py::test_lagged_reconnects_and_resumes_from_cursor` |
| 4 | Dedup by `id`; ignore `seq <= cursor` | `test_cursor.py::test_dedups_by_id_across_replay_live_boundary`, `test_mock_server.py::test_dedup_across_replay_live_boundary` |
| 5 | Advance/persist cursor only after handling | `test_cursor.py::test_cursor_advances_only_via_record`, `test_integration.py::test_resume_after_disconnect_replays_missed_exactly_once` |
| 6 | `lagged` / ws-error → reconnect + resume from cursor | `test_mock_server.py::test_lagged_reconnects_and_resumes_from_cursor`, `test_integration.py::test_resume_after_disconnect_replays_missed_exactly_once` |
| 7 | `replay_gap` = explicit, logged data-loss | `test_mock_server.py::test_ok_before_replay_gap_ordering` |
| 8 | `closing`: `revoked` fatal, `shutdown` backoff-reconnect | `test_mock_server.py::test_revoked_closing_is_fatal`, `test_mock_server.py::test_shutdown_closing_reconnects` |
| 9 | Answer WebSocket ping frames | provided by the `websockets` keepalive; exercised by idle long-lived connections in `test_mock_server.py::test_clean_cancellation_of_events_iterator` |
| 10 | Ignore unknown frame `type`s and unknown fields | `test_frames.py::test_skips_unknown_frame_type`, `test_frames.py::test_tolerates_unknown_fields_on_known_frame`, `test_mock_server.py::test_unknown_frame_type_ignored_over_the_wire` |

## License

MIT OR Apache-2.0.
