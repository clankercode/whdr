(* Integration tests against a REAL whdr-server (current master, durable
   delivery). Boots the prebuilt binary against a temp config, mints a token
   over the control socket, ingests events via HTTP, and drives the client.
   Skips gracefully when the binaries are absent. *)

open Lwt.Infix
module K = Whdr_sub_kit
module Client = Whdr_sub_kit.Client
module J = Yojson.Safe

let tc name f = Alcotest_lwt.test_case name `Quick f
let server_bin = "/home/xertrov/src/whdr/target/debug/whdr-server"
let fake_ext_bin = "/home/xertrov/src/whdr/target/debug/examples/whdr-ext-fake"
let binaries_present () = Sys.file_exists server_bin && Sys.file_exists fake_ext_bin

let with_timeout ~secs p =
  Lwt.pick
    [ p; (Lwt_unix.sleep secs >>= fun () -> Lwt.fail (Failure "integration step timed out")) ]

(* ------------------------------------------------------------- collector *)

type col = { mutable events : K.Frame.delivered_event list; mutable unavailable : string list }

let fresh () = { events = []; unavailable = [] }
let seqs c = List.rev_map (fun (e : K.Frame.delivered_event) -> e.seq) c.events

let payloads c =
  List.rev_map
    (fun (e : K.Frame.delivered_event) ->
      match K.Frame.payload e with Ok s -> s | Error _ -> "<decode-error>")
    c.events

let make_handler ~target c =
  let d, u = Lwt.wait () in
  let bump () =
    if List.length c.events >= target && match Lwt.state d with Lwt.Sleep -> true | _ -> false
    then Lwt.wakeup_later u ()
  in
  let h =
    Client.handler
      ~on_event:(fun e ->
        c.events <- e :: c.events;
        bump ();
        Lwt.return_unit)
      ~on_replay_unavailable:(fun m ->
        c.unavailable <- m :: c.unavailable;
        Lwt.return_unit)
      ()
  in
  (h, d)

(* Run [client] until [done_t] resolves or a fatal error; then stop it. *)
let run_until client handler done_t =
  let ran = Client.run client handler in
  with_timeout ~secs:15.0 (Lwt.pick [ (ran >|= fun r -> `Ran r); (done_t >|= fun () -> `Done) ])

(* ---------------------------------------------------------------- server *)

type server = { dir : string; ingest_port : int; sub_port : int; pid : int }

let write_file path content =
  let oc = open_out path in
  output_string oc content;
  close_out oc

let copy_file src dst =
  let ic = open_in_bin src and oc = open_out_bin dst in
  let len = 65536 in
  let buf = Bytes.create len in
  let rec loop () =
    let n = input ic buf 0 len in
    if n > 0 then (
      output oc buf 0 n;
      loop ())
  in
  loop ();
  close_in ic;
  close_out oc

let mktemp () =
  let base = Filename.temp_file "whdrml" "" in
  Sys.remove base;
  Unix.mkdir base 0o700;
  base

let starts_with ~prefix s =
  String.length s >= String.length prefix && String.sub s 0 (String.length prefix) = prefix

let sub_url server = Printf.sprintf "ws://127.0.0.1:%d/subscribe" server.sub_port

let spawn ~delivery () =
  let dir = mktemp () in
  let exts = Filename.concat dir "exts" in
  Unix.mkdir exts 0o700;
  let dst = Filename.concat exts "whdr-ext-alpha" in
  copy_file fake_ext_bin dst;
  Unix.chmod dst 0o755;
  write_file (Filename.concat exts "whdr-ext-alpha.toml") "";
  let secrets = Filename.concat dir "secrets.toml" in
  write_file secrets "alpha = \"secret-alpha\"\n";
  Unix.chmod secrets 0o600;
  let ingest_port = Ws_server.free_port () in
  let sub_port = Ws_server.free_port () in
  let metrics_port = Ws_server.free_port () in
  let ctl = Filename.concat dir "ctl.sock" in
  let delivery_block =
    if delivery then
      Printf.sprintf "[delivery]\nenabled = true\nstore_path = \"%s\"\n\n"
        (Filename.concat dir "delivery.redb")
    else ""
  in
  let config = Filename.concat dir "config.toml" in
  write_file config
    (Printf.sprintf
       "[server]\n\
        listen_addr = \"127.0.0.1:%d\"\n\
        sub_addr = \"127.0.0.1:%d\"\n\
        metrics_addr = \"127.0.0.1:%d\"\n\
        control_socket = \"%s\"\n\n\
        [subscribers]\n\
        token_store = \"%s\"\n\n\
        [extensions]\n\
        enabled = [\"alpha\"]\n\n\
        [limits]\n\n\
        [timeouts]\n\n\
        %s[secrets]\n\
        file = \"%s\"\n"
       ingest_port sub_port metrics_port ctl
       (Filename.concat dir "tokens.toml")
       delivery_block secrets);
  let logfd =
    Unix.openfile (Filename.concat dir "server.log") [ O_WRONLY; O_CREAT; O_APPEND ] 0o644
  in
  let path = exts ^ ":" ^ try Sys.getenv "PATH" with Not_found -> "" in
  let env =
    Array.of_list
      (("PATH=" ^ path)
      :: List.filter
           (fun s -> not (starts_with ~prefix:"PATH=" s))
           (Array.to_list (Unix.environment ())))
  in
  let pid =
    Unix.create_process_env server_bin
      [| server_bin; "--config"; config |]
      env Unix.stdin logfd logfd
  in
  Unix.close logfd;
  { dir; ingest_port; sub_port; pid }

let control server req =
  Lwt_io.with_connection
    (Unix.ADDR_UNIX (Filename.concat server.dir "ctl.sock"))
    (fun (ic, oc) ->
      Lwt_io.write_line oc (J.to_string req) >>= fun () ->
      Lwt_io.flush oc >>= fun () -> Lwt_io.read_line ic)
  >|= J.from_string

let status server = control server (`Assoc [ ("type", `String "status") ])

let rec wait_ready server deadline =
  Lwt.catch (fun () -> status server >|= fun _ -> true) (fun _ -> Lwt.return false) >>= function
  | true -> Lwt.return_unit
  | false ->
      if Unix.gettimeofday () > deadline then Lwt.fail (Failure "server did not become ready")
      else Lwt_unix.sleep 0.05 >>= fun () -> wait_ready server deadline

let ext_ready st =
  match J.Util.(st |> member "status" |> member "extensions") with
  | `List exts ->
      List.exists
        (fun e ->
          J.Util.(e |> member "id" |> to_string_option) = Some "alpha"
          && J.Util.(e |> member "state" |> to_string_option) = Some "Ready")
        exts
  | _ -> false

let rec wait_ext server deadline =
  status server >>= fun st ->
  if ext_ready st then Lwt.return_unit
  else if Unix.gettimeofday () > deadline then Lwt.fail (Failure "ext alpha never became Ready")
  else Lwt_unix.sleep 0.05 >>= fun () -> wait_ext server deadline

let boot ~delivery () =
  let server = spawn ~delivery () in
  let deadline = Unix.gettimeofday () +. 15.0 in
  wait_ready server deadline >>= fun () ->
  wait_ext server deadline >>= fun () -> Lwt.return server

let token_add server name =
  control server (`Assoc [ ("type", `String "token.add"); ("name", `String name) ]) >|= fun resp ->
  J.Util.(resp |> member "token" |> to_string)

let http_post server path body =
  Lwt_io.with_connection
    (Unix.ADDR_INET (Unix.inet_addr_loopback, server.ingest_port))
    (fun (ic, oc) ->
      let req =
        Printf.sprintf
          "POST %s HTTP/1.1\r\nHost: t\r\nContent-Length: %d\r\nConnection: close\r\n\r\n" path
          (String.length body)
      in
      Lwt_io.write oc req >>= fun () ->
      Lwt_io.write oc body >>= fun () ->
      Lwt_io.flush oc >>= fun () -> Lwt_io.read ic)
  >|= fun resp ->
  match String.split_on_char ' ' resp with
  | _ :: code :: _ -> ( try int_of_string code with _ -> 0)
  | _ -> 0

let stop server =
  (try Unix.kill server.pid Sys.sigterm with _ -> ());
  Lwt.catch
    (fun () -> with_timeout ~secs:8.0 (Lwt_unix.waitpid [] server.pid >|= ignore))
    (fun _ ->
      (try Unix.kill server.pid Sys.sigkill with _ -> ());
      Lwt.return_unit)
  >|= fun () -> ignore (Sys.command (Printf.sprintf "rm -rf %s" (Filename.quote server.dir)))

let with_server ~delivery body =
  boot ~delivery () >>= fun server -> Lwt.finalize (fun () -> body server) (fun () -> stop server)

(* ----------------------------------------------------------------- tests *)

(* Live subscribe: connect, then ingest, and receive the event live. *)
let live_subscribe _switch () =
  with_server ~delivery:true (fun server ->
      token_add server "p" >>= fun token ->
      let c = fresh () in
      let handler, done_t = make_handler ~target:1 c in
      let client = Client.create ~patterns:[ "alpha.>" ] ~url:(sub_url server) ~token () in
      (* Subscribe first, then emit — the event should arrive. *)
      Lwt.async (fun () ->
          Lwt_unix.sleep 0.4 >>= fun () ->
          http_post server "/alpha" "live-body" >|= fun _ -> ());
      run_until client handler done_t >|= fun _ ->
      Alcotest.(check (list int64)) "one event" [ 1L ] (seqs c);
      Alcotest.(check (list string)) "payload" [ "live-body" ] (payloads c);
      Alcotest.(check string) "channel" "alpha.echo" (List.nth (List.rev c.events) 0).channel)

(* Resume after disconnect replays missed events exactly-once at the handler. *)
let resume_exactly_once _switch () =
  with_server ~delivery:true (fun server ->
      token_add server "p" >>= fun token ->
      (* Two events with no subscriber connected: persisted seq 1,2. *)
      http_post server "/alpha" "one" >>= fun s1 ->
      http_post server "/alpha" "two" >>= fun s2 ->
      Alcotest.(check bool) "ingest 200s" true (s1 = 200 && s2 = 200);
      let cursor_store = K.Cursor_store.memory 0L in
      let c1 = fresh () in
      let h1, d1 = make_handler ~target:2 c1 in
      let client1 =
        Client.create ~cursor_store ~patterns:[ "alpha.>" ] ~url:(sub_url server) ~token ()
      in
      run_until client1 h1 d1 >>= fun _ ->
      Alcotest.(check (list int64)) "replayed 1,2" [ 1L; 2L ] (seqs c1);
      Alcotest.(check (list string)) "payloads one,two" [ "one"; "two" ] (payloads c1);
      (* A third event after the first client stopped. *)
      http_post server "/alpha" "three" >>= fun _ ->
      let c2 = fresh () in
      let h2, d2 = make_handler ~target:1 c2 in
      (* Same cursor store => resumes from cursor 2, gets only seq 3. *)
      let client2 =
        Client.create ~cursor_store ~patterns:[ "alpha.>" ] ~url:(sub_url server) ~token ()
      in
      run_until client2 h2 d2 >|= fun _ ->
      Alcotest.(check (list int64)) "only seq 3 on resume (exactly-once)" [ 3L ] (seqs c2);
      Alcotest.(check (list string)) "payload three" [ "three" ] (payloads c2))

(* Durability disabled: replay request => error op replay; live-only continues. *)
let durability_disabled _switch () =
  with_server ~delivery:false (fun server ->
      token_add server "p" >>= fun token ->
      let c = fresh () in
      let handler, done_t = make_handler ~target:1 c in
      (* resume_cursor 0 => the client sends replay.after_seq = 0. *)
      let client = Client.create ~patterns:[ "alpha.>" ] ~url:(sub_url server) ~token () in
      Lwt.async (fun () ->
          Lwt_unix.sleep 0.4 >>= fun () ->
          http_post server "/alpha" "live-only" >|= fun _ -> ());
      run_until client handler done_t >|= fun _ ->
      Alcotest.(check bool) "replay refused (error op replay)" true (c.unavailable <> []);
      Alcotest.(check (list int64)) "live event still delivered" [ 1L ] (seqs c);
      Alcotest.(check (list string)) "payload" [ "live-only" ] (payloads c))

(* Bad token => fatal auth error, no websocket established. *)
let bad_token_fatal _switch () =
  with_server ~delivery:true (fun server ->
      let client =
        Client.create ~patterns:[ "alpha.>" ] ~url:(sub_url server) ~token:"tok_bogus" ()
      in
      with_timeout ~secs:10.0 (Client.connect client) >|= function
      | Error K.Error.Auth -> ()
      | Error e -> Alcotest.failf "expected Auth, got %s" (K.Error.to_string e)
      | Ok _ -> Alcotest.fail "bad token unexpectedly connected")

let tests =
  if binaries_present () then
    [
      ( "integration",
        [
          tc "live subscribe" live_subscribe;
          tc "resume replays exactly-once" resume_exactly_once;
          tc "durability disabled: error op replay, live-only" durability_disabled;
          tc "bad token is fatal auth error" bad_token_fatal;
        ] );
    ]
  else
    [
      ( "integration",
        [
          tc "skipped: whdr-server binaries absent" (fun _ () ->
              Printf.printf "\n  [integration skipped: %s not found]\n%!" server_bin;
              Lwt.return_unit);
        ] );
    ]
