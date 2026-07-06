(** Cursor persistence hook.

    Provide one to make at-least-once delivery survive process restarts: [load] is called once at
    {!Client.run} start, and [save] is called after each event is successfully handled. If you only
    need not-missing-while-briefly-disconnected, the in-memory store ({!memory}) is enough.

    Both hooks are [Lwt] thunks. Raising (via [Lwt.fail]) from either is fatal: a client that cannot
    persist its cursor cannot honour its at-least-once contract, so {!Client.run} stops with
    {!Error.Cursor_store}. *)

type t = {
  load : unit -> int64 Lwt.t;
      (** load the last persisted cursor (0 = replay from the start of retention) *)
  save : int64 -> unit Lwt.t;  (** persist a cursor value after a successful handle *)
}

val memory : int64 -> t
(** An in-memory store seeded with [initial]. Does not survive process restarts. *)
