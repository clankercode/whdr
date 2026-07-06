type closing_reason = Shutdown | Revoked | Unknown_reason of string

type delivered_event = {
  id : string;
  seq : int64;
  ts_ms : int64;
  channel : string;
  payload_b64 : string;
}

type server_msg =
  | Welcome of string
  | Ack of string
  | Server_error of { op : string; msg : string }
  | Event of delivered_event
  | Replayed of int64
  | Replay_gap of { from_seq : int64; earliest_seq : int64 }
  | Lagged of int64
  | Pong
  | Closing of closing_reason

type client_msg =
  | Subscribe of { patterns : string list; after_seq : int64 option }
  | Unsubscribe of string list
  | Ping

(* JSON numbers up to u64 must survive: yojson parses large integers that do
   not fit a native [int] as [`Intlit]. Accept every numeric shape. *)
let int64_of_decimal s =
  (* [Int64.of_string] rejects u64 values above [Int64.max_int]; the [0u] prefix
     parses them as unsigned, bit-preserving (u64 max -> -1L). *)
  try Some (Int64.of_string s) with _ -> ( try Some (Int64.of_string ("0u" ^ s)) with _ -> None)

let to_int64 = function
  | `Int i -> Some (Int64.of_int i)
  | `Intlit s -> int64_of_decimal s
  | `Float f -> Some (Int64.of_float f)
  | _ -> None

let parse_server_frame text =
  match Yojson.Safe.from_string text with
  | exception _ -> None
  | `Assoc fields -> (
      let get k = List.assoc_opt k fields in
      let str k = match get k with Some (`String s) -> Some s | _ -> None in
      let i64 k = match get k with Some v -> to_int64 v | None -> None in
      let ( let* ) = Option.bind in
      match str "type" with
      | Some "welcome" -> Option.map (fun n -> Welcome n) (str "name")
      | Some "ok" -> Option.map (fun op -> Ack op) (str "op")
      | Some "error" ->
          let* op = str "op" in
          let* msg = str "msg" in
          Some (Server_error { op; msg })
      | Some "event" ->
          let* id = str "id" in
          let* seq = i64 "seq" in
          let* ts_ms = i64 "ts_ms" in
          let* channel = str "channel" in
          let* payload_b64 = str "payload_b64" in
          Some (Event { id; seq; ts_ms; channel; payload_b64 })
      | Some "replayed" -> Option.map (fun s -> Replayed s) (i64 "through_seq")
      | Some "replay_gap" ->
          let* from_seq = i64 "from_seq" in
          let* earliest_seq = i64 "earliest_seq" in
          Some (Replay_gap { from_seq; earliest_seq })
      | Some "lagged" -> Option.map (fun d -> Lagged d) (i64 "dropped")
      | Some "pong" -> Some Pong
      | Some "closing" ->
          let reason =
            match str "reason" with
            | Some "shutdown" -> Shutdown
            | Some "revoked" -> Revoked
            | Some other -> Unknown_reason other
            | None -> Unknown_reason ""
          in
          Some (Closing reason)
      (* Unknown [type] tags (conformance item 10) and frames missing required
         fields are ignored. *)
      | Some _ | None -> None)
  | _ -> None

let client_msg_to_json = function
  | Subscribe { patterns; after_seq } ->
      let base =
        [
          ("type", `String "subscribe"); ("patterns", `List (List.map (fun p -> `String p) patterns));
        ]
      in
      let base =
        match after_seq with
        | Some s -> base @ [ ("replay", `Assoc [ ("after_seq", `Intlit (Int64.to_string s)) ]) ]
        | None -> base
      in
      `Assoc base
  | Unsubscribe patterns ->
      `Assoc
        [
          ("type", `String "unsubscribe");
          ("patterns", `List (List.map (fun p -> `String p) patterns));
        ]
  | Ping -> `Assoc [ ("type", `String "ping") ]

let client_msg_to_string m = Yojson.Safe.to_string (client_msg_to_json m)
let payload ev = match Base64.decode ev.payload_b64 with Ok s -> Ok s | Error (`Msg m) -> Error m
let closing_is_fatal = function Revoked -> true | Shutdown | Unknown_reason _ -> false
