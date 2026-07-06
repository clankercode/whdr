(* Pure unit tests: frame parsing (incl. unknown-type tolerance), backoff,
   cursor/dedup, cursor store. *)

open Whdr_sub_kit

let tc name f = Alcotest_lwt.test_case name `Quick f
let ret () = Lwt.return_unit

(* ---------------------------------------------------------------- Frame *)

let event_frame () =
  let text =
    {|{"type":"event","id":"00000000-0000-0000-0000-000000000000","seq":7,"ts_ms":1751760000000,"channel":"github.push","payload_b64":"AA=="}|}
  in
  match Frame.parse_server_frame text with
  | Some (Frame.Event e) ->
      Alcotest.(check int64) "seq" 7L e.seq;
      Alcotest.(check string) "channel" "github.push" e.channel;
      Alcotest.(check int64) "ts_ms" 1751760000000L e.ts_ms
  | _ -> Alcotest.fail "expected event frame"

let big_u64_seq () =
  (* A seq beyond 2^53 must survive (yojson emits [`Intlit]). *)
  let text =
    {|{"type":"event","id":"x","seq":18446744073709551615,"ts_ms":0,"channel":"c","payload_b64":"AA=="}|}
  in
  match Frame.parse_server_frame text with
  | Some (Frame.Event e) -> Alcotest.(check int64) "u64 max as -1L" (-1L) e.seq
  | _ -> Alcotest.fail "expected event frame"

let unknown_type () =
  (* Conformance item 10: unknown [type] values are ignored. *)
  Alcotest.(check bool)
    "unknown type" true
    (Frame.parse_server_frame {|{"type":"quantum_flux","foo":1}|} = None);
  Alcotest.(check bool) "not json" true (Frame.parse_server_frame "not json at all" = None)

let unknown_fields () =
  (* Conformance item 10: unknown object fields on a known frame are ignored. *)
  match
    Frame.parse_server_frame {|{"type":"welcome","name":"p","future_field":{"nested":true}}|}
  with
  | Some (Frame.Welcome n) -> Alcotest.(check string) "name" "p" n
  | _ -> Alcotest.fail "expected welcome"

let all_server_frames () =
  let ck label text pred =
    match Frame.parse_server_frame text with
    | Some m -> Alcotest.(check bool) label true (pred m)
    | None -> Alcotest.failf "%s: parse failed" label
  in
  ck "ok" {|{"type":"ok","op":"subscribe"}|} (function Frame.Ack "subscribe" -> true | _ -> false);
  ck "error" {|{"type":"error","op":"replay","msg":"nope"}|} (function
    | Frame.Server_error { op = "replay"; msg = "nope" } -> true
    | _ -> false);
  ck "replayed" {|{"type":"replayed","through_seq":42}|} (function
    | Frame.Replayed 42L -> true
    | _ -> false);
  ck "replay_gap" {|{"type":"replay_gap","from_seq":10,"earliest_seq":57}|} (function
    | Frame.Replay_gap { from_seq = 10L; earliest_seq = 57L } -> true
    | _ -> false);
  ck "lagged" {|{"type":"lagged","dropped":3}|} (function Frame.Lagged 3L -> true | _ -> false);
  ck "pong" {|{"type":"pong"}|} (function Frame.Pong -> true | _ -> false);
  ck "closing shutdown" {|{"type":"closing","reason":"shutdown"}|} (function
    | Frame.Closing Frame.Shutdown -> true
    | _ -> false);
  ck "closing revoked" {|{"type":"closing","reason":"revoked"}|} (function
    | Frame.Closing Frame.Revoked -> true
    | _ -> false);
  ck "closing unknown reason preserved" {|{"type":"closing","reason":"martian"}|} (function
    | Frame.Closing (Frame.Unknown_reason "martian") -> true
    | _ -> false)

let closing_fatality () =
  (* Conformance item 8. *)
  Alcotest.(check bool) "revoked fatal" true (Frame.closing_is_fatal Frame.Revoked);
  Alcotest.(check bool) "shutdown not fatal" false (Frame.closing_is_fatal Frame.Shutdown);
  Alcotest.(check bool)
    "unknown not fatal" false
    (Frame.closing_is_fatal (Frame.Unknown_reason "x"))

let payload_decode () =
  let e : Frame.delivered_event =
    { id = "i"; seq = 1L; ts_ms = 0L; channel = "c"; payload_b64 = "aGVsbG8=" }
  in
  match Frame.payload e with
  | Ok s -> Alcotest.(check string) "decoded" "hello" s
  | Error m -> Alcotest.failf "decode failed: %s" m

let client_msg_serialisation () =
  (* Conformance item 3: resume with replay.after_seq = cursor. *)
  let with_cursor =
    Frame.client_msg_to_string
      (Frame.Subscribe { patterns = [ "github.>" ]; after_seq = Some 128L })
  in
  let j = Yojson.Safe.from_string with_cursor in
  let after = Yojson.Safe.Util.(j |> member "replay" |> member "after_seq") in
  Alcotest.(check string) "after_seq preserved" "128" (Yojson.Safe.to_string after);
  Alcotest.(check string) "type" "subscribe" Yojson.Safe.Util.(j |> member "type" |> to_string);
  (* Live-only when no cursor: no [replay] key. *)
  let live =
    Frame.client_msg_to_string (Frame.Subscribe { patterns = [ "a.>" ]; after_seq = None })
  in
  let jl = Yojson.Safe.from_string live in
  Alcotest.(check bool) "no replay key" true (Yojson.Safe.Util.member "replay" jl = `Null)

let frame_tests =
  [
    tc "event frame carries seq/channel/ts" (fun _ () ->
        event_frame ();
        ret ());
    tc "big u64 seq survives" (fun _ () ->
        big_u64_seq ();
        ret ());
    tc "unknown type ignored" (fun _ () ->
        unknown_type ();
        ret ());
    tc "unknown fields tolerated" (fun _ () ->
        unknown_fields ();
        ret ());
    tc "all server frames parse" (fun _ () ->
        all_server_frames ();
        ret ());
    tc "closing fatality" (fun _ () ->
        closing_fatality ();
        ret ());
    tc "payload decodes" (fun _ () ->
        payload_decode ();
        ret ());
    tc "client msg serialisation" (fun _ () ->
        client_msg_serialisation ();
        ret ());
  ]

(* -------------------------------------------------------------- Backoff *)

let backoff_grows_and_caps () =
  let policy = { Backoff.initial = 0.5; max = 8.0; multiplier = 2.0; jitter = 0.0 } in
  let b = Backoff.start policy in
  let f = Alcotest.(check (float 1e-9)) in
  f "0.5" 0.5 (Backoff.next_delay b);
  f "1.0" 1.0 (Backoff.next_delay b);
  f "2.0" 2.0 (Backoff.next_delay b);
  f "4.0" 4.0 (Backoff.next_delay b);
  f "8.0 cap" 8.0 (Backoff.next_delay b);
  f "8.0 cap again" 8.0 (Backoff.next_delay b)

let backoff_reset () =
  let b = Backoff.start { Backoff.default_policy with jitter = 0.0 } in
  let first = Backoff.next_delay b in
  ignore (Backoff.next_delay b);
  ignore (Backoff.next_delay b);
  Backoff.reset b;
  Alcotest.(check (float 1e-9)) "reset to initial" first (Backoff.next_delay b)

let backoff_jitter_bounds () =
  let base = 1.0 in
  let lo = Backoff.apply_jitter ~base ~jitter:0.2 ~rand01:0.0 in
  let hi = Backoff.apply_jitter ~base ~jitter:0.2 ~rand01:0.9999 in
  Alcotest.(check bool) "lo >= 0.8" true (lo >= 0.8 -. 1e-9 && lo <= base +. 1e-9);
  Alcotest.(check bool) "hi < 1.2" true (hi >= base -. 1e-9 && hi < 1.2);
  Alcotest.(check (float 1e-9))
    "zero jitter exact" base
    (Backoff.apply_jitter ~base ~jitter:0.0 ~rand01:0.5)

let backoff_tests =
  [
    tc "delays grow and cap" (fun _ () ->
        backoff_grows_and_caps ();
        ret ());
    tc "reset returns to initial" (fun _ () ->
        backoff_reset ();
        ret ());
    tc "jitter stays within bounds" (fun _ () ->
        backoff_jitter_bounds ();
        ret ());
  ]

(* --------------------------------------------------------------- Resume *)

let resume_skips_below_cursor () =
  let s = Resume.create ~cursor:0L ~capacity:16 in
  Alcotest.(check bool) "process 1" true (Resume.should_process s ~id:"a" ~seq:1L);
  Resume.record s ~id:"a" ~seq:1L;
  Alcotest.(check int64) "cursor 1" 1L (Resume.cursor s);
  Alcotest.(check bool) "dup seq 1 skipped" false (Resume.should_process s ~id:"a" ~seq:1L);
  Alcotest.(check bool) "new id lower seq skipped" false (Resume.should_process s ~id:"z" ~seq:1L);
  Alcotest.(check bool) "higher seq proceeds" true (Resume.should_process s ~id:"b" ~seq:2L)

let resume_dedup_by_id () =
  let s = Resume.create ~cursor:5L ~capacity:16 in
  Alcotest.(check bool) "process replay id" true (Resume.should_process s ~id:"g" ~seq:6L);
  Resume.record s ~id:"g" ~seq:6L;
  Alcotest.(check bool) "same id skipped" false (Resume.should_process s ~id:"g" ~seq:6L);
  Alcotest.(check bool) "same id higher seq skipped" false (Resume.should_process s ~id:"g" ~seq:8L)

let resume_cursor_only_via_record () =
  let s = Resume.create ~cursor:10L ~capacity:16 in
  Alcotest.(check bool) "asking ok" true (Resume.should_process s ~id:"a" ~seq:11L);
  Alcotest.(check int64) "cursor unchanged by ask" 10L (Resume.cursor s);
  Resume.record s ~id:"a" ~seq:11L;
  Alcotest.(check int64) "cursor advanced" 11L (Resume.cursor s)

let resume_bounded_seen () =
  let s = Resume.create ~cursor:0L ~capacity:2 in
  Resume.record s ~id:"1" ~seq:1L;
  Resume.record s ~id:"2" ~seq:2L;
  Resume.record s ~id:"3" ~seq:3L (* evicts id 1 *);
  (* id 1 evicted from seen, but its seq (1) is below the cursor (3) so still guarded. *)
  Alcotest.(check bool)
    "evicted id still guarded by cursor" false
    (Resume.should_process s ~id:"1" ~seq:1L);
  Alcotest.(check bool) "id 2 still remembered" false (Resume.should_process s ~id:"2" ~seq:2L)

let resume_tests =
  [
    tc "skips seq at/below cursor" (fun _ () ->
        resume_skips_below_cursor ();
        ret ());
    tc "dedups by id across boundary" (fun _ () ->
        resume_dedup_by_id ();
        ret ());
    tc "cursor advances only via record" (fun _ () ->
        resume_cursor_only_via_record ();
        ret ());
    tc "bounded seen set evicts oldest" (fun _ () ->
        resume_bounded_seen ();
        ret ());
  ]

(* ---------------------------------------------------------- Cursor store *)

let cursor_store_round_trip _ () =
  let open Lwt.Infix in
  let s = Cursor_store.memory 42L in
  s.load () >>= fun v ->
  Alcotest.(check int64) "initial" 42L v;
  s.save 100L >>= fun () ->
  s.load () >>= fun v2 ->
  Alcotest.(check int64) "saved" 100L v2;
  Lwt.return_unit

let cursor_tests = [ tc "memory store round trips" cursor_store_round_trip ]

let tests =
  [
    ("frame", frame_tests);
    ("backoff", backoff_tests);
    ("resume", resume_tests);
    ("cursor_store", cursor_tests);
  ]
