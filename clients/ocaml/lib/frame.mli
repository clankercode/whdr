(** Frame parsing and the delivered-event view.

    These are the pure, transport-agnostic building blocks the {!Connection} and {!Client.run} loop
    are built from — kept free of I/O so every conformance rule has a unit test. Parsing tolerates
    unknown [type] tags and unknown object fields (forward compatibility, conformance item 10). *)

(** Reason carried by a [closing] frame. [Unknown_reason] preserves an unrecognised reason so future
    values do not crash the client. *)
type closing_reason = Shutdown | Revoked | Unknown_reason of string

type delivered_event = {
  id : string;
  seq : int64;
  ts_ms : int64;
  channel : string;
  payload_b64 : string;
}
(** A delivered event, decoded from an [event] frame.

    [id] is stable across live delivery and every replay of the event — {b dedup by [id]}. [seq] is
    the global monotonic cursor key. [ts_ms] is the server wall-clock at fan-out; it is
    informational — order by [seq], not [ts_ms]. *)

(** A typed server -> client frame. *)
type server_msg =
  | Welcome of string  (** first frame after auth; the subscriber name *)
  | Ack of string  (** [ok] acknowledging an op (the op name) *)
  | Server_error of { op : string; msg : string }
      (** an op failed; non-fatal. [op = "replay"] means durability is off *)
  | Event of delivered_event
  | Replayed of int64  (** replay window fully delivered up to [through_seq] *)
  | Replay_gap of { from_seq : int64; earliest_seq : int64 }
      (** requested cursor predates retention; the interior span is pruned *)
  | Lagged of int64  (** outbound queue evicted [dropped] events *)
  | Pong
  | Closing of closing_reason

(** A typed client -> server frame. *)
type client_msg =
  | Subscribe of { patterns : string list; after_seq : int64 option }
      (** [after_seq = Some c] resumes replay from cursor [c]; [None] is live-only. *)
  | Unsubscribe of string list
  | Ping

val parse_server_frame : string -> server_msg option
(** Parse one text frame into a typed message, returning [None] for unknown [type] tags and
    otherwise-undecodable frames (the caller ignores the frame and reads the next one). Unknown
    object fields on a known frame are tolerated. *)

val client_msg_to_string : client_msg -> string
(** Serialise a client message to a JSON string suitable for a WebSocket text frame. *)

val payload : delivered_event -> (string, string) result
(** Decode [payload_b64] to raw bytes. *)

val closing_is_fatal : closing_reason -> bool
(** Whether a [closing] reason is fatal to the {!Client.run} loop: [Revoked] is fatal (obtain a new
    token); [Shutdown] and unknown reasons are transient (reconnect with backoff). Conformance item
    8. *)
