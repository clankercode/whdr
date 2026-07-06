(* A tiny hand-rolled RFC 6455 WebSocket server for protocol-order tests.

   [start handler] listens on a free loopback port and invokes
   [handler conn_index conn] once per accepted connection ([conn_index] counts
   from 0), letting a test script the exact server frame sequence — including
   "ok before replay_gap" ordering and multi-connection reconnect flows. No
   third-party ws dependency, so the shared opam switch is left untouched. *)

open Lwt.Infix

type conn = { ic : Lwt_io.input_channel; oc : Lwt_io.output_channel }
type t = { port : int; stop : unit Lwt.u }

let op_text = 0x1
let op_close = 0x8
let op_ping = 0x9
let op_pong = 0xA

let free_port () =
  let s = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Unix.setsockopt s Unix.SO_REUSEADDR true;
  Unix.bind s (Unix.ADDR_INET (Unix.inet_addr_loopback, 0));
  let port = match Unix.getsockname s with Unix.ADDR_INET (_, p) -> p | _ -> 0 in
  Unix.close s;
  port

let mask_payload key s =
  let b = Bytes.of_string s in
  for i = 0 to Bytes.length b - 1 do
    Bytes.set b i (Char.chr (Char.code (Bytes.get b i) lxor Char.code key.[i land 3]))
  done;
  Bytes.unsafe_to_string b

(* Server frames are NOT masked (RFC 6455 5.1). *)
let write_frame oc ~opcode payload =
  let len = String.length payload in
  let buf = Buffer.create (len + 4) in
  Buffer.add_char buf (Char.chr (0x80 lor opcode));
  if len < 126 then Buffer.add_char buf (Char.chr len)
  else if len < 65536 then (
    Buffer.add_char buf (Char.chr 126);
    Buffer.add_char buf (Char.chr ((len lsr 8) land 0xff));
    Buffer.add_char buf (Char.chr (len land 0xff)))
  else (
    Buffer.add_char buf (Char.chr 127);
    for i = 7 downto 0 do
      Buffer.add_char buf (Char.chr ((len lsr (i * 8)) land 0xff))
    done);
  Buffer.add_string buf payload;
  Lwt_io.write oc (Buffer.contents buf) >>= fun () -> Lwt_io.flush oc

let read_exactly ic n =
  if n = 0 then Lwt.return ""
  else
    let b = Bytes.create n in
    Lwt_io.read_into_exactly ic b 0 n >>= fun () -> Lwt.return (Bytes.unsafe_to_string b)

let read_byte ic = Lwt_io.read_char ic >|= Char.code

let read_frame ic =
  read_byte ic >>= fun b0 ->
  read_byte ic >>= fun b1 ->
  let opcode = b0 land 0x0f in
  let masked = b1 land 0x80 <> 0 in
  let len0 = b1 land 0x7f in
  (if len0 < 126 then Lwt.return len0
   else if len0 = 126 then
     read_exactly ic 2 >|= fun s -> (Char.code s.[0] lsl 8) lor Char.code s.[1]
   else
     read_exactly ic 8 >|= fun s ->
     let v = ref 0 in
     String.iter (fun c -> v := (!v lsl 8) lor Char.code c) s;
     !v)
  >>= fun len ->
  (if masked then read_exactly ic 4 else Lwt.return "") >>= fun key ->
  read_exactly ic len >>= fun payload ->
  Lwt.return (opcode, if masked then mask_payload key payload else payload)

let send_text conn s = write_frame conn.oc ~opcode:op_text s
let send_ping conn s = write_frame conn.oc ~opcode:op_ping s

(* Raw next frame (opcode, payload) — used to assert on the client's Pong. *)
let recv_frame conn = read_frame conn.ic

(* Next text frame content, answering the client's pings. *)
let rec recv_text conn =
  read_frame conn.ic >>= fun (op, payload) ->
  if op = op_text then Lwt.return payload
  else if op = op_ping then write_frame conn.oc ~opcode:op_pong payload >>= fun () -> recv_text conn
  else if op = op_close then Lwt.fail End_of_file
  else recv_text conn

let forever () = fst (Lwt.wait ())

let event_json ~seq ~id ~channel ~payload_b64 =
  Printf.sprintf {|{"type":"event","id":"%s","seq":%d,"ts_ms":0,"channel":"%s","payload_b64":"%s"}|}
    id seq channel payload_b64

let parse_after_seq sub_text =
  match Yojson.Safe.from_string sub_text with
  | exception _ -> None
  | j -> (
      match Yojson.Safe.Util.(j |> member "replay" |> member "after_seq") with
      | `Int i -> Some (Int64.of_int i)
      | `Intlit s -> ( try Some (Int64.of_string s) with _ -> None)
      | _ -> None)

(* ---- handshake ---- *)

let ws_accept key =
  let guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11" in
  Base64.encode_exn Digestif.SHA1.(to_raw_string (digest_string (key ^ guid)))

let lower s = String.lowercase_ascii s

let handshake ic oc =
  let rec read_headers key =
    Lwt_io.read_line ic >>= fun line ->
    let line = String.trim line in
    if line = "" then Lwt.return key
    else
      let key =
        match String.index_opt line ':' with
        | Some i when lower (String.trim (String.sub line 0 i)) = "sec-websocket-key" ->
            String.trim (String.sub line (i + 1) (String.length line - i - 1))
        | _ -> key
      in
      read_headers key
  in
  read_headers "" >>= fun key ->
  Lwt_io.write oc
    (Printf.sprintf
       "HTTP/1.1 101 Switching Protocols\r\n\
        Upgrade: websocket\r\n\
        Connection: Upgrade\r\n\
        Sec-WebSocket-Accept: %s\r\n\
        \r\n"
       (ws_accept key))
  >>= fun () -> Lwt_io.flush oc

let serve_conn i cfd handler =
  let ic = Lwt_io.of_fd ~mode:Lwt_io.Input cfd in
  let oc = Lwt_io.of_fd ~mode:Lwt_io.Output cfd in
  Lwt.finalize
    (fun () -> handshake ic oc >>= fun () -> handler i { ic; oc })
    (fun () ->
      Lwt.catch (fun () -> Lwt_io.close oc) (fun _ -> Lwt.return_unit) >>= fun () ->
      Lwt.catch (fun () -> Lwt_io.close ic) (fun _ -> Lwt.return_unit))

let start (handler : int -> conn -> unit Lwt.t) =
  let s = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
  Unix.setsockopt s Unix.SO_REUSEADDR true;
  Unix.bind s (Unix.ADDR_INET (Unix.inet_addr_loopback, 0));
  Unix.listen s 16;
  let port = match Unix.getsockname s with Unix.ADDR_INET (_, p) -> p | _ -> 0 in
  let lfd = Lwt_unix.of_unix_file_descr s in
  let stop_t, stop_u = Lwt.wait () in
  let conn = ref (-1) in
  let rec accept_loop () =
    Lwt.pick [ (Lwt_unix.accept lfd >|= fun x -> `A x); (stop_t >|= fun () -> `Stop) ] >>= function
    | `Stop -> Lwt.catch (fun () -> Lwt_unix.close lfd) (fun _ -> Lwt.return_unit)
    | `A (cfd, _addr) ->
        incr conn;
        let i = !conn in
        Lwt.async (fun () ->
            Lwt.catch (fun () -> serve_conn i cfd handler) (fun _ -> Lwt.return_unit));
        accept_loop ()
  in
  Lwt.async accept_loop;
  { port; stop = stop_u }

let url t = Printf.sprintf "ws://127.0.0.1:%d/subscribe" t.port

let shutdown t =
  (try Lwt.wakeup_later t.stop () with _ -> ());
  Lwt.return_unit
