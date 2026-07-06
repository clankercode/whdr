open Lwt.Infix

type handler = {
  on_event : Frame.delivered_event -> unit Lwt.t;
  on_replayed : int64 -> unit Lwt.t;
  on_replay_gap : from_seq:int64 -> earliest_seq:int64 -> unit Lwt.t;
  on_lagged : int64 -> unit Lwt.t;
  on_replay_unavailable : string -> unit Lwt.t;
}

let handler ?(on_replayed = fun _ -> Lwt.return_unit)
    ?(on_replay_gap = fun ~from_seq:_ ~earliest_seq:_ -> Lwt.return_unit)
    ?(on_lagged = fun _ -> Lwt.return_unit) ?(on_replay_unavailable = fun _ -> Lwt.return_unit)
    ~on_event () =
  { on_event; on_replayed; on_replay_gap; on_lagged; on_replay_unavailable }

type t = {
  url : string;
  token : string;
  patterns : string list;
  backoff : Backoff.policy;
  cursor_store : Cursor_store.t;
  dedup_capacity : int;
}

let create ?(patterns = []) ?(backoff = Backoff.default_policy) ?cursor_store ?(resume_cursor = 0L)
    ?(dedup_capacity = 8192) ~url ~token () =
  let cursor_store =
    match cursor_store with Some s -> s | None -> Cursor_store.memory resume_cursor
  in
  { url; token; patterns; backoff; cursor_store; dedup_capacity = max 1 dedup_capacity }

(* Run a possibly-raising Lwt thunk, mapping a raised exception through [wrap]. *)
let guard f wrap =
  Lwt.catch (fun () -> f () >|= fun v -> Ok v) (fun exn -> Lwt.return (Error (wrap exn)))

(* Run a handler hook, mapping any raise to a fatal handler error. *)
let hook f = guard f (fun exn -> Error.Handler exn)
let load_cursor t = guard t.cursor_store.load (fun exn -> Error.Cursor_store exn)
let save_cursor t c = guard (fun () -> t.cursor_store.save c) (fun exn -> Error.Cursor_store exn)

let connect t =
  load_cursor t >>= function
  | Error e -> Lwt.return (Error e)
  | Ok cursor -> (
      Connection.connect ~url:t.url ~token:t.token >>= function
      | Error e -> Lwt.return (Error e)
      | Ok conn -> (
          Connection.subscribe conn ~patterns:t.patterns ~after_seq:(Some cursor) >>= function
          | Error e -> Connection.close conn >>= fun () -> Lwt.return (Error e)
          | Ok () -> Lwt.return (Ok conn)))

(* One connection's lifetime. [Ok ()] means "reconnect and resume" (clean close,
   [shutdown], or [lagged]); a fatal [Error] stops the loop. *)
let run_session t handler resume backoff =
  Connection.connect ~url:t.url ~token:t.token >>= function
  | Error e -> Lwt.return (Error e)
  | Ok conn -> (
      (* Connected: reset backoff so a later drop reconnects fast. *)
      Backoff.reset backoff;
      let finish result = Connection.close conn >>= fun () -> Lwt.return result in
      let dispatch msg loop =
        match msg with
        | Frame.Event ev ->
            if Resume.should_process resume ~id:ev.id ~seq:ev.seq then
              hook (fun () -> handler.on_event ev) >>= function
              | Error e -> finish (Error e)
              | Ok () -> (
                  Resume.record resume ~id:ev.id ~seq:ev.seq;
                  save_cursor t (Resume.cursor resume) >>= function
                  | Error e -> finish (Error e)
                  | Ok () -> loop ())
            else loop ()
        | Frame.Replayed through_seq -> (
            hook (fun () -> handler.on_replayed through_seq) >>= function
            | Error e -> finish (Error e)
            | Ok () -> loop ())
        | Frame.Replay_gap { from_seq; earliest_seq } -> (
            hook (fun () -> handler.on_replay_gap ~from_seq ~earliest_seq) >>= function
            | Error e -> finish (Error e)
            | Ok () -> loop ())
        | Frame.Lagged dropped -> (
            hook (fun () -> handler.on_lagged dropped) >>= function
            | Error e -> finish (Error e)
            (* Recover by reconnecting and replaying from the cursor. *)
            | Ok () -> finish (Ok ()))
        | Frame.Server_error { op; msg } ->
            if op = "replay" then
              hook (fun () -> handler.on_replay_unavailable msg) >>= function
              | Error e -> finish (Error e)
              | Ok () -> loop ()
            else loop () (* non-fatal (e.g. bad pattern); connection stays open *)
        | Frame.Closing reason ->
            if Frame.closing_is_fatal reason then finish (Error Error.Revoked)
            else finish (Ok ()) (* shutdown: reconnect with backoff *)
        | Frame.Welcome _ | Frame.Ack _ | Frame.Pong -> loop ()
      in
      let rec loop () =
        Connection.recv conn >>= function
        | Error e -> finish (Error e)
        | Ok msg -> dispatch msg loop
      in
      Connection.subscribe conn ~patterns:t.patterns ~after_seq:(Some (Resume.cursor resume))
      >>= function
      | Error e -> finish (Error e)
      | Ok () -> loop ())

let run t handler =
  load_cursor t >>= function
  | Error e -> Lwt.return (Error e)
  | Ok cursor ->
      let resume = Resume.create ~cursor ~capacity:t.dedup_capacity in
      let backoff = Backoff.start t.backoff in
      let rec outer () =
        run_session t handler resume backoff >>= function
        | Error e when Error.is_fatal e -> Lwt.return (Error e)
        | Ok () | Error _ ->
            let delay = Backoff.next_delay backoff in
            Lwt_unix.sleep delay >>= outer
      in
      outer ()
