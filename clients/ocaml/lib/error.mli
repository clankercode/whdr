(** Errors surfaced by the subscriber client.

    {!is_fatal} distinguishes {e fatal} errors (the {!Client.run} loop stops and returns them) from
    {e transient} errors (the loop reconnects with backoff). The reconnect-and-resume algorithm
    (SPEC 9.4) treats an authentication failure, a [revoked] close, a handler error, and a
    cursor-store failure as fatal; everything else — a dropped socket, a server [shutdown], a
    [lagged] eviction — is transient. *)

type t =
  | Auth
      (** The WebSocket upgrade was rejected with HTTP [401]: token missing, wrong, or revoked.
          {b Fatal.} *)
  | Http of int  (** The upgrade failed with a non-401 HTTP status. {b Transient.} *)
  | Revoked  (** The server sent [closing] with reason [revoked]. {b Fatal.} *)
  | Handler of exn  (** The application event handler raised. {b Fatal.} *)
  | Cursor_store of exn  (** A cursor-persistence hook raised. {b Fatal.} *)
  | Transport of string  (** The connection dropped or the transport errored. {b Transient.} *)
  | Connection_closed  (** The connection closed cleanly (or via a [Close] frame). {b Transient.} *)
  | Request of string  (** Building the connection request (URL / header) failed. {b Fatal.} *)

val is_fatal : t -> bool
(** Whether the {!Client.run} loop should stop and return this error instead of reconnecting. *)

val to_string : t -> string
(** A human-readable description, for logging. *)
