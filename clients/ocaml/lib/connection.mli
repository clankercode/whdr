(** The typed WebSocket connection: authenticated upgrade, welcome handshake, and a frame-by-frame
    typed stream.

    A {!t} is positioned just after the [welcome] frame. {!recv} transparently answers WebSocket
    ping frames (conformance item 9) and skips unrecognised frames (conformance item 10). *)

type t

val connect : url:string -> token:string -> (t, Error.t) result Lwt.t
(** Connect to [url] (e.g. [ws://host:port/subscribe]), send [Authorization: Bearer <token>] on the
    upgrade (conformance item 1), and consume the [welcome] frame before returning (conformance item
    2).

    A [401] upgrade rejection maps to {!Error.Auth}; other HTTP statuses to {!Error.Http}. Only the
    plaintext [ws://] scheme is supported (the whdr server is plaintext); a [wss://] url returns
    {!Error.Request} — terminate TLS at a reverse proxy and connect over [ws://]. *)

val name : t -> string
(** The subscriber name echoed in the [welcome] frame (the token's label). *)

val subscribe : t -> patterns:string list -> after_seq:int64 option -> (unit, Error.t) result Lwt.t
(** Send a [subscribe]. [after_seq = Some cursor] requests replay from [cursor] (conformance item
    3); [None] is live-only. *)

val ping : t -> (unit, Error.t) result Lwt.t
(** Send an application-level [ping] ([{"type":"ping"}]). *)

val send : t -> Frame.client_msg -> (unit, Error.t) result Lwt.t
(** Send an arbitrary client message. *)

val recv : t -> (Frame.server_msg, Error.t) result Lwt.t
(** Read the next typed server frame, answering WebSocket pings inline and skipping unrecognised
    frames. Returns {!Error.Connection_closed} when the peer closes. *)

val close : t -> unit Lwt.t
(** Best-effort close: send a Close frame and shut the transport down. *)
