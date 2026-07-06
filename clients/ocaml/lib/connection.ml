(* A minimal, self-contained RFC 6455 WebSocket client over Lwt_unix TCP.

   We hand-roll the client (handshake + text/close/ping/pong framing with client
   masking) rather than depend on a third-party ws library, to keep the
   dependency footprint to already-present, non-conflicting packages
   (lwt, yojson, base64, uri). Only the plaintext [ws://] scheme is supported;
   for TLS, terminate at a reverse proxy and connect over [ws://] (the whdr
   server itself is plaintext per the wire-protocol appendix). *)

open Lwt.Infix

type t = { ic : Lwt_io.input_channel; oc : Lwt_io.output_channel; mutable name : string }

let name t = t.name

(* ------------------------------------------------------------ framing *)

let op_text = 0x1
let op_close = 0x8
let op_ping = 0x9
let op_pong = 0xA

let mask_payload key s =
  let b = Bytes.of_string s in
  for i = 0 to Bytes.length b - 1 do
    Bytes.set b i (Char.chr (Char.code (Bytes.get b i) lxor Char.code key.[i land 3]))
  done;
  Bytes.unsafe_to_string b

(* Client frames MUST be masked (RFC 6455 5.3). *)
let write_frame oc ~opcode payload =
  let len = String.length payload in
  let buf = Buffer.create (len + 8) in
  Buffer.add_char buf (Char.chr (0x80 lor opcode));
  if len < 126 then Buffer.add_char buf (Char.chr (0x80 lor len))
  else if len < 65536 then (
    Buffer.add_char buf (Char.chr (0x80 lor 126));
    Buffer.add_char buf (Char.chr ((len lsr 8) land 0xff));
    Buffer.add_char buf (Char.chr (len land 0xff)))
  else (
    Buffer.add_char buf (Char.chr (0x80 lor 127));
    for i = 7 downto 0 do
      Buffer.add_char buf (Char.chr ((len lsr (i * 8)) land 0xff))
    done);
  let key = Bytes.create 4 in
  for i = 0 to 3 do
    Bytes.set key i (Char.chr (Random.int 256))
  done;
  let key = Bytes.unsafe_to_string key in
  Buffer.add_string buf key;
  Buffer.add_string buf (mask_payload key payload);
  Lwt_io.write oc (Buffer.contents buf) >>= fun () -> Lwt_io.flush oc

let read_exactly ic n =
  if n = 0 then Lwt.return ""
  else
    let b = Bytes.create n in
    Lwt_io.read_into_exactly ic b 0 n >>= fun () -> Lwt.return (Bytes.unsafe_to_string b)

let read_byte ic = Lwt_io.read_char ic >|= Char.code

(* Read one frame, returning [(opcode, payload)]. Unmasks if the peer masked
   (servers usually do not). *)
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

(* --------------------------------------------------------- public ops *)

let rec recv t =
  Lwt.catch
    (fun () -> read_frame t.ic >|= fun f -> `F f)
    (function
      | End_of_file -> Lwt.return `Closed
      | exn -> Lwt.return (`E (Error.Transport (Printexc.to_string exn))))
  >>= function
  | `Closed -> Lwt.return (Error Error.Connection_closed)
  | `E e -> Lwt.return (Error e)
  | `F (opcode, payload) ->
      if opcode = op_text then
        match Frame.parse_server_frame payload with
        | Some m -> Lwt.return (Ok m)
        | None -> recv t (* unknown frame: keep reading *)
      else if opcode = op_ping then
        (* Conformance item 9: answer WebSocket pings. *)
        Lwt.catch (fun () -> write_frame t.oc ~opcode:op_pong payload) (fun _ -> Lwt.return_unit)
        >>= fun () -> recv t
      else if opcode = op_close then Lwt.return (Error Error.Connection_closed)
      else recv t (* pong / continuation / binary: ignore *)

let send t msg =
  Lwt.catch
    (fun () ->
      write_frame t.oc ~opcode:op_text (Frame.client_msg_to_string msg) >|= fun () -> Ok ())
    (fun exn -> Lwt.return (Error (Error.Transport (Printexc.to_string exn))))

let subscribe t ~patterns ~after_seq = send t (Frame.Subscribe { patterns; after_seq })
let ping t = send t Frame.Ping

let sec_key () =
  let b = Bytes.create 16 in
  for i = 0 to 15 do
    Bytes.set b i (Char.chr (Random.int 256))
  done;
  Base64.encode_exn (Bytes.unsafe_to_string b)

let resolve_addr host port =
  Lwt_unix.getaddrinfo host (string_of_int port) [ Unix.AI_SOCKTYPE Unix.SOCK_STREAM ] >>= function
  | { Unix.ai_addr; _ } :: _ -> Lwt.return ai_addr
  | [] -> Lwt.fail (Failure ("cannot resolve host: " ^ host))

let status_code line =
  match String.split_on_char ' ' (String.trim line) with
  | _ :: c :: _ -> ( try int_of_string (String.trim c) with _ -> 0)
  | _ -> 0

let connect ~url ~token =
  let uri = Uri.of_string url in
  match Uri.host uri with
  | None | Some "" -> Lwt.return (Error (Error.Request ("missing host in url: " ^ url)))
  | Some host -> (
      if Uri.scheme uri = Some "wss" then
        Lwt.return
          (Error
             (Error.Request
                "wss (TLS) is not supported by this transport; terminate TLS at a proxy and \
                 connect via ws://"))
      else
        let port = Option.value ~default:80 (Uri.port uri) in
        let path = match Uri.path uri with "" -> "/" | p -> p in
        Lwt.catch
          (fun () ->
            resolve_addr host port >>= fun sockaddr ->
            let fd = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
            Lwt.catch
              (fun () -> Lwt_unix.connect fd sockaddr)
              (fun exn -> Lwt_unix.close fd >>= fun () -> Lwt.fail exn)
            >>= fun () ->
            let ic = Lwt_io.of_fd ~mode:Lwt_io.Input fd in
            let oc = Lwt_io.of_fd ~mode:Lwt_io.Output fd in
            let req =
              Printf.sprintf
                "GET %s HTTP/1.1\r\n\
                 Host: %s:%d\r\n\
                 Upgrade: websocket\r\n\
                 Connection: Upgrade\r\n\
                 Sec-WebSocket-Key: %s\r\n\
                 Sec-WebSocket-Version: 13\r\n\
                 Authorization: Bearer %s\r\n\
                 \r\n"
                path host port (sec_key ()) token
            in
            Lwt_io.write oc req >>= fun () ->
            Lwt_io.flush oc >>= fun () ->
            Lwt_io.read_line ic >>= fun status_line ->
            let code = status_code status_line in
            if code <> 101 then
              Lwt.catch (fun () -> Lwt_io.close ic) (fun _ -> Lwt.return_unit) >|= fun () ->
              `Status code
            else
              (* Drain remaining response headers up to the blank line. *)
              let rec drain () =
                Lwt_io.read_line ic >>= fun l ->
                if String.trim l = "" then Lwt.return_unit else drain ()
              in
              drain () >|= fun () -> `Ok { ic; oc; name = "" })
          (fun exn -> Lwt.return (`Exn exn))
        >>= function
        | `Exn exn -> Lwt.return (Error (Error.Transport (Printexc.to_string exn)))
        | `Status 401 -> Lwt.return (Error Error.Auth)
        | `Status code -> Lwt.return (Error (Error.Http code))
        | `Ok t ->
            let rec await_welcome () =
              recv t >>= function
              | Ok (Frame.Welcome n) ->
                  t.name <- n;
                  Lwt.return (Ok t)
              | Ok _ -> await_welcome ()
              | Error e -> Lwt.return (Error e)
            in
            await_welcome ())

let close t =
  Lwt.catch (fun () -> write_frame t.oc ~opcode:op_close "") (fun _ -> Lwt.return_unit)
  >>= fun () ->
  Lwt.catch (fun () -> Lwt_io.close t.oc) (fun _ -> Lwt.return_unit) >>= fun () ->
  Lwt.catch (fun () -> Lwt_io.close t.ic) (fun _ -> Lwt.return_unit)
