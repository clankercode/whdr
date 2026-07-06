(* Test entry point for whdr_sub_kit.

   Conformance map — Subscriber wire protocol v2 appendix, section 9 checklist
   (item -> covering test(s)):

   1. Authorization: Bearer on upgrade; 401 fatal.
        integration "bad token is fatal auth error" (401 -> Error.Auth);
        every integration test authenticates with a real minted token.
   2. Waits for welcome before subscribing.
        Connection.connect consumes welcome first; exercised by every protocol
        and integration test (the scripted server sends welcome, then reads the
        subscribe).
   3. Sends subscribe with replay.after_seq = cursor on every (re)connect.
        unit "client msg serialisation"; protocol "lagged reconnects and
        resumes from cursor" (asserts after_seq = [0; 2]); integration "resume
        replays exactly-once".
   4. Dedups by id; ignores seq <= cursor.
        unit "skips seq at/below cursor", "dedups by id across boundary";
        protocol "dedup by id across boundary"; integration "resume replays
        exactly-once".
   5. Advances and (optionally) persists cursor only after handling.
        unit "cursor advances only via record"; integration "resume replays
        exactly-once" (cursor persisted across two runs via a shared store).
   6. lagged and ws error -> reconnect + resume from cursor.
        protocol "lagged reconnects and resumes from cursor", "closing shutdown
        reconnects".
   7. replay_gap is an explicit, logged data-loss signal.
        protocol "ok before replay_gap, replay then live" (gap surfaced to
        on_replay_gap).
   8. Handles closing (revoked -> fatal; shutdown -> backoff reconnect).
        unit "closing fatality"; protocol "closing revoked is fatal", "closing
        shutdown reconnects".
   9. Answers WebSocket ping frames.
        protocol "client answers ws ping"; integration tests also answer the
        real server's ws pings during longer waits.
   10. Ignores unknown frame types and unknown fields.
        unit "unknown type ignored", "unknown fields tolerated", "all server
        frames parse"; protocol "ok before replay_gap..." (an unknown frame is
        injected mid-stream and ignored). *)

let () =
  Lwt_main.run
    (Alcotest_lwt.run "whdr_sub_kit"
       (Unit_tests.tests @ Protocol_tests.tests @ Integration_tests.tests))
