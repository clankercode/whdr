(** The configured subscriber client and the batteries-included reconnect-and-resume loop.

    {!run} performs the full appendix section 7 algorithm: auth -> welcome -> subscribe with
    [replay.after_seq = cursor] -> dedup by [id]/[seq] -> advance the cursor after each successful
    handle -> recover from [lagged] / disconnects by reconnecting from the cursor -> surface
    [replay_gap] -> treat [revoked] as fatal and [shutdown] as a backoff reconnect. *)

type handler = {
  on_event : Frame.delivered_event -> unit Lwt.t;
      (** Handle a delivered event. Already de-duplicated by [id] and [seq], so called at most once
          per event. On return, the cursor advances to [event.seq]. *)
  on_replayed : int64 -> unit Lwt.t;
      (** A replay window finished; live frames follow. The argument is the head [through_seq] the
          connection caught up to. *)
  on_replay_gap : from_seq:int64 -> earliest_seq:int64 -> unit Lwt.t;
      (** {b Explicit data-loss signal.} Events in [(from_seq, earliest_seq)] are permanently gone.
      *)
  on_lagged : int64 -> unit Lwt.t;
      (** The server evicted [dropped] events; {!run} reconnects and replays from the cursor to
          recover. *)
  on_replay_unavailable : string -> unit Lwt.t;
      (** A [replay] request was refused because durability is disabled; live delivery still works.
      *)
}
(** Application callbacks for {!run}. Build with {!handler}.

    Only [on_event] is required. Raising (via [Lwt.fail]) from any hook is {b fatal}: {!run} stops
    and returns {!Error.Handler}. The cursor advances (and is persisted) only {e after} [on_event]
    returns, giving at-least-once delivery. *)

val handler :
  ?on_replayed:(int64 -> unit Lwt.t) ->
  ?on_replay_gap:(from_seq:int64 -> earliest_seq:int64 -> unit Lwt.t) ->
  ?on_lagged:(int64 -> unit Lwt.t) ->
  ?on_replay_unavailable:(string -> unit Lwt.t) ->
  on_event:(Frame.delivered_event -> unit Lwt.t) ->
  unit ->
  handler
(** Build a {!handler}. Unset hooks default to no-ops. *)

type t
(** A configured subscriber client. *)

val create :
  ?patterns:string list ->
  ?backoff:Backoff.policy ->
  ?cursor_store:Cursor_store.t ->
  ?resume_cursor:int64 ->
  ?dedup_capacity:int ->
  url:string ->
  token:string ->
  unit ->
  t
(** Configure a client for the [/subscribe] endpoint [url], authenticating with [token].

    - [patterns]: channel patterns to subscribe (default none).
    - [backoff]: reconnect policy (default {!Backoff.default_policy}).
    - [cursor_store]: cursor-persistence hook. If omitted, an in-memory store seeded from
      [resume_cursor] is used.
    - [resume_cursor]: initial cursor when no [cursor_store] is given ([0L] = replay from the start
      of retention).
    - [dedup_capacity]: recent-[id] dedup window size (default 8192). *)

val connect : t -> (Connection.t, Error.t) result Lwt.t
(** Connect, authenticate, and subscribe with the configured patterns and cursor (loaded from the
    cursor store). Returns a ready {!Connection.t} for bespoke loops; most callers want {!run}. *)

val run : t -> handler -> (unit, Error.t) result Lwt.t
(** Run the full reconnect-and-resume loop, driving [handler]. Loops forever, reconnecting with
    exponential backoff after a transient failure (dropped socket, server [shutdown], [lagged]
    eviction). Returns only on a {b fatal} error: a revoked/absent token, a handler failure, or a
    cursor-store failure. *)
