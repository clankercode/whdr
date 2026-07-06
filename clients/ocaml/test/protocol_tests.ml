(* Protocol-order tests driving the real {!Client.run} loop against the
   in-process scriptable ws server. Covers reconnect-and-resume, dedup,
   ok-before-replay_gap ordering, lagged recovery, closing semantics, and the
   ws-ping answer. *)

open Lwt.Infix
module K = Whdr_sub_kit
module Client = Whdr_sub_kit.Client

let tc name f = Alcotest_lwt.test_case name `Quick f
let fast_backoff = { K.Backoff.initial = 0.02; max = 0.05; multiplier = 2.0; jitter = 0.0 }

type collected = {
  mutable events : K.Frame.delivered_event list; (* reversed *)
  mutable replayed : int64 list;
  mutable gaps : (int64 * int64) list;
  mutable lagged : int64 list;
  mutable replay_unavailable : string list;
}

let fresh () = { events = []; replayed = []; gaps = []; lagged = []; replay_unavailable = [] }
let seqs col = List.rev_map (fun (e : K.Frame.delivered_event) -> e.seq) col.events
let ids col = List.rev_map (fun (e : K.Frame.delivered_event) -> e.id) col.events
let sleeping p = match Lwt.state p with Lwt.Sleep -> true | _ -> false

(* Build a handler that resolves [done_t] once [target] events are collected. *)
let make_handler ~target col =
  let done_t, done_u = Lwt.wait () in
  let bump () =
    if List.length col.events >= target && sleeping done_t then Lwt.wakeup_later done_u ()
  in
  let h =
    K.Client.handler
      ~on_event:(fun ev ->
        col.events <- ev :: col.events;
        bump ();
        Lwt.return_unit)
      ~on_replayed:(fun s ->
        col.replayed <- s :: col.replayed;
        Lwt.return_unit)
      ~on_replay_gap:(fun ~from_seq ~earliest_seq ->
        col.gaps <- (from_seq, earliest_seq) :: col.gaps;
        Lwt.return_unit)
      ~on_lagged:(fun d ->
        col.lagged <- d :: col.lagged;
        Lwt.return_unit)
      ~on_replay_unavailable:(fun m ->
        col.replay_unavailable <- m :: col.replay_unavailable;
        Lwt.return_unit)
      ()
  in
  (h, done_t)

let with_timeout ~secs p =
  Lwt.pick [ p; (Lwt_unix.sleep secs >>= fun () -> Lwt.fail (Failure "test timed out")) ]

(* Run the client until [done_t] resolves, then shut the server down. Returns
   unit; raises on timeout. *)
let drive server client handler done_t =
  let ran = Client.run client handler in
  Lwt.finalize
    (fun () ->
      with_timeout ~secs:5.0 (Lwt.pick [ (ran >|= fun _ -> ()); (done_t >|= fun () -> ()) ]))
    (fun () -> Ws_server.shutdown server)

let welcome = {|{"type":"welcome","name":"p"}|}
let ok = {|{"type":"ok","op":"subscribe"}|}
let ev ~seq ~id = Ws_server.event_json ~seq ~id ~channel:"alpha.echo" ~payload_b64:"AA=="

(* ---- 1. ok -> replay_gap -> events -> replayed -> (unknown) -> live ---- *)
let ordering_test _switch () =
  let col = fresh () in
  let handler, done_t = make_handler ~target:3 col in
  let script _i client =
    Ws_server.send_text client welcome >>= fun () ->
    Ws_server.recv_text client >>= fun _sub ->
    Ws_server.send_text client ok >>= fun () ->
    Ws_server.send_text client {|{"type":"replay_gap","from_seq":1,"earliest_seq":5}|} >>= fun () ->
    Ws_server.send_text client (ev ~seq:5 ~id:"id5") >>= fun () ->
    Ws_server.send_text client (ev ~seq:6 ~id:"id6") >>= fun () ->
    Ws_server.send_text client {|{"type":"replayed","through_seq":6}|} >>= fun () ->
    Ws_server.send_text client {|{"type":"quantum_flux","x":1}|}
    (* unknown: ignored *) >>= fun () ->
    Ws_server.send_text client (ev ~seq:7 ~id:"id7") >>= fun () -> Ws_server.forever ()
  in
  let server = Ws_server.start script in
  let client =
    Client.create ~patterns:[ "alpha.>" ] ~backoff:fast_backoff ~url:(Ws_server.url server)
      ~token:"tok_x" ()
  in
  drive server client handler done_t >|= fun () ->
  Alcotest.(check (list int64)) "events in order" [ 5L; 6L; 7L ] (seqs col);
  Alcotest.(check (list (pair int64 int64))) "gap surfaced" [ (1L, 5L) ] col.gaps;
  Alcotest.(check (list int64)) "replayed" [ 6L ] col.replayed

(* ---- 2. dedup by id across the replay/live boundary ---- *)
let dedup_test _switch () =
  let col = fresh () in
  let handler, done_t = make_handler ~target:2 col in
  let script _i client =
    Ws_server.send_text client welcome >>= fun () ->
    Ws_server.recv_text client >>= fun _sub ->
    Ws_server.send_text client ok >>= fun () ->
    Ws_server.send_text client (ev ~seq:5 ~id:"A") >>= fun () ->
    Ws_server.send_text client (ev ~seq:5 ~id:"A") (* duplicate: dropped *) >>= fun () ->
    Ws_server.send_text client (ev ~seq:6 ~id:"B") >>= fun () -> Ws_server.forever ()
  in
  let server = Ws_server.start script in
  let client =
    Client.create ~patterns:[ "alpha.>" ] ~backoff:fast_backoff ~url:(Ws_server.url server)
      ~token:"tok_x" ()
  in
  drive server client handler done_t >|= fun () ->
  Alcotest.(check (list int64)) "no duplicate seq" [ 5L; 6L ] (seqs col);
  Alcotest.(check (list string)) "distinct ids" [ "A"; "B" ] (ids col)

(* ---- 3. lagged -> reconnect -> resume from cursor ---- *)
let lagged_resume_test _switch () =
  let col = fresh () in
  let handler, done_t = make_handler ~target:4 col in
  let after_seqs = ref [] in
  let script i client =
    Ws_server.send_text client welcome >>= fun () ->
    Ws_server.recv_text client >>= fun sub ->
    after_seqs := Ws_server.parse_after_seq sub :: !after_seqs;
    Ws_server.send_text client ok >>= fun () ->
    if i = 0 then
      Ws_server.send_text client (ev ~seq:1 ~id:"a") >>= fun () ->
      Ws_server.send_text client (ev ~seq:2 ~id:"b") >>= fun () ->
      Ws_server.send_text client {|{"type":"lagged","dropped":3}|} >>= fun () ->
      Ws_server.forever ()
    else
      Ws_server.send_text client (ev ~seq:3 ~id:"c") >>= fun () ->
      Ws_server.send_text client (ev ~seq:4 ~id:"d") >>= fun () -> Ws_server.forever ()
  in
  let server = Ws_server.start script in
  let client =
    Client.create ~patterns:[ "alpha.>" ] ~backoff:fast_backoff ~url:(Ws_server.url server)
      ~token:"tok_x" ()
  in
  drive server client handler done_t >|= fun () ->
  Alcotest.(check (list int64)) "all events after recovery" [ 1L; 2L; 3L; 4L ] (seqs col);
  Alcotest.(check (list int64)) "lagged observed" [ 3L ] col.lagged;
  match List.rev !after_seqs with
  | [ Some 0L; Some 2L ] -> ()
  | other ->
      Alcotest.failf "expected resume cursors [0;2], got [%s]"
        (String.concat ";"
           (List.map (function Some s -> Int64.to_string s | None -> "none") other))

(* ---- 4. closing revoked is fatal ---- *)
let revoked_fatal_test _switch () =
  let col = fresh () in
  let handler, _done = make_handler ~target:99 col in
  let script _i client =
    Ws_server.send_text client welcome >>= fun () ->
    Ws_server.recv_text client >>= fun _sub ->
    Ws_server.send_text client ok >>= fun () ->
    Ws_server.send_text client {|{"type":"closing","reason":"revoked"}|} >>= fun () ->
    Ws_server.forever ()
  in
  let server = Ws_server.start script in
  let client =
    Client.create ~patterns:[ "alpha.>" ] ~backoff:fast_backoff ~url:(Ws_server.url server)
      ~token:"tok_x" ()
  in
  Lwt.finalize
    (fun () -> with_timeout ~secs:5.0 (Client.run client handler))
    (fun () -> Ws_server.shutdown server)
  >|= fun result ->
  match result with
  | Error K.Error.Revoked -> ()
  | Error e -> Alcotest.failf "expected Revoked, got %s" (K.Error.to_string e)
  | Ok () -> Alcotest.fail "expected fatal Revoked, got Ok"

(* ---- 5. closing shutdown reconnects ---- *)
let shutdown_reconnect_test _switch () =
  let col = fresh () in
  let handler, done_t = make_handler ~target:1 col in
  let script i client =
    Ws_server.send_text client welcome >>= fun () ->
    Ws_server.recv_text client >>= fun _sub ->
    Ws_server.send_text client ok >>= fun () ->
    if i = 0 then
      Ws_server.send_text client {|{"type":"closing","reason":"shutdown"}|} >>= fun () ->
      Ws_server.forever ()
    else Ws_server.send_text client (ev ~seq:1 ~id:"a") >>= fun () -> Ws_server.forever ()
  in
  let server = Ws_server.start script in
  let client =
    Client.create ~patterns:[ "alpha.>" ] ~backoff:fast_backoff ~url:(Ws_server.url server)
      ~token:"tok_x" ()
  in
  drive server client handler done_t >|= fun () ->
  Alcotest.(check (list int64)) "delivered after shutdown reconnect" [ 1L ] (seqs col)

(* ---- 6. non-replay error is non-fatal; delivery continues ---- *)
let nonfatal_error_test _switch () =
  let col = fresh () in
  let handler, done_t = make_handler ~target:1 col in
  let script _i client =
    Ws_server.send_text client welcome >>= fun () ->
    Ws_server.recv_text client >>= fun _sub ->
    Ws_server.send_text client {|{"type":"error","op":"subscribe","msg":"invalid pattern"}|}
    >>= fun () ->
    Ws_server.send_text client (ev ~seq:1 ~id:"a") >>= fun () -> Ws_server.forever ()
  in
  let server = Ws_server.start script in
  let client =
    Client.create ~patterns:[ "alpha.>" ] ~backoff:fast_backoff ~url:(Ws_server.url server)
      ~token:"tok_x" ()
  in
  drive server client handler done_t >|= fun () ->
  Alcotest.(check (list int64)) "event delivered despite prior error" [ 1L ] (seqs col);
  Alcotest.(check (list string)) "no replay-unavailable" [] col.replay_unavailable

(* ---- 7. client answers a server ws ping ---- *)
let ws_ping_answer_test _switch () =
  let col = fresh () in
  let handler, done_t = make_handler ~target:1 col in
  let pong_ok = ref false in
  let script _i client =
    Ws_server.send_text client welcome >>= fun () ->
    Ws_server.recv_text client >>= fun _sub ->
    Ws_server.send_text client ok >>= fun () ->
    Ws_server.send_ping client "hb" >>= fun () ->
    Ws_server.recv_frame client >>= fun (op, content) ->
    if op = Ws_server.op_pong then pong_ok := content = "hb";
    Ws_server.send_text client (ev ~seq:1 ~id:"a") >>= fun () -> Ws_server.forever ()
  in
  let server = Ws_server.start script in
  let client =
    Client.create ~patterns:[ "alpha.>" ] ~backoff:fast_backoff ~url:(Ws_server.url server)
      ~token:"tok_x" ()
  in
  drive server client handler done_t >|= fun () ->
  Alcotest.(check bool) "client answered ws ping with matching pong" true !pong_ok;
  Alcotest.(check (list int64)) "delivery continued after ping" [ 1L ] (seqs col)

let tests =
  [
    ( "protocol",
      [
        tc "ok before replay_gap, replay then live" ordering_test;
        tc "dedup by id across boundary" dedup_test;
        tc "lagged reconnects and resumes from cursor" lagged_resume_test;
        tc "closing revoked is fatal" revoked_fatal_test;
        tc "closing shutdown reconnects" shutdown_reconnect_test;
        tc "non-replay error is non-fatal" nonfatal_error_test;
        tc "client answers ws ping" ws_ping_answer_test;
      ] );
  ]
