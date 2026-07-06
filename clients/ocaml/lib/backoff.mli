(** Exponential backoff with jitter for reconnect scheduling.

    Delays follow [initial *. multiplier ** attempt], capped at [max], then multiplied by a random
    factor in [\[1 -. jitter, 1 +. jitter)]. All durations are in {b seconds}. A fresh {!t} (see
    {!start}) resets to [attempt = 0]; the {!Client.run} loop resets it after every successful
    connection so a long-lived connection that later drops reconnects fast. *)

type policy = {
  initial : float;  (** delay before the first reconnect attempt (seconds) *)
  max : float;  (** upper bound on the pre-jitter delay (seconds) *)
  multiplier : float;  (** growth factor applied per attempt *)
  jitter : float;  (** jitter fraction in [\[0., 1.)]; [0.2] = +/-20% *)
}

val default_policy : policy
(** [{ initial = 0.5; max = 30.; multiplier = 2.; jitter = 0.2 }]. *)

type t
(** Running state for a {!policy}. *)

val start : policy -> t
(** Begin a fresh backoff run at [attempt = 0]. *)

val reset : t -> unit
(** Reset to the initial delay (call after a successful connection). *)

val next_delay : t -> float
(** Compute the next delay (with jitter, in seconds) and advance the attempt counter. *)

val base_delay : policy -> int -> float
(** The deterministic (pre-jitter) base delay for an attempt number. Exposed for testing. *)

val apply_jitter : base:float -> jitter:float -> rand01:float -> float
(** Pure jitter application, factored out for testability. [rand01] is a sample in [\[0, 1)]. *)
