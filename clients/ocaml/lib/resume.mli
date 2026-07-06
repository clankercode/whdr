(** Cursor + dedup state implementing the appendix's at-least-once guard.

    This is the heart of the conformance checklist's dedup rule (items 4 and 5): an event is
    processed at most once, and the cursor advances only {e after} a successful handle. [seq] is a
    {b global} monotonic counter, so gaps in the [seq] values a connection observes are normal (they
    belong to other subscribers' patterns) — never infer loss from a gap. *)

type t

val create : cursor:int64 -> capacity:int -> t
(** Create state resuming from [cursor], remembering up to [capacity] recent ids for replay/live
    boundary dedup ([capacity] is clamped to at least 1). *)

val cursor : t -> int64
(** The highest [seq] successfully processed so far — the value to send as [replay.after_seq] on the
    next (re)connect. *)

val should_process : t -> id:string -> seq:int64 -> bool
(** Whether an event with this [id]/[seq] should be handed to the handler. Skips a [seq] at or below
    the cursor, or an [id] already processed within the recent window. *)

val record : t -> id:string -> seq:int64 -> unit
(** Record a successfully-handled event: remember its [id] (evicting the oldest beyond [capacity])
    and advance the cursor. *)
