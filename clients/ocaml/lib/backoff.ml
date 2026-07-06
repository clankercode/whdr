type policy = { initial : float; max : float; multiplier : float; jitter : float }

let default_policy = { initial = 0.5; max = 30.; multiplier = 2.; jitter = 0.2 }

type t = { policy : policy; mutable attempt : int }

let start policy = { policy; attempt = 0 }
let reset t = t.attempt <- 0
let base_delay p attempt = Float.min (p.initial *. (p.multiplier ** float_of_int attempt)) p.max

let apply_jitter ~base ~jitter ~rand01 =
  if jitter <= 0. then base
  else
    (* Map rand01 in [0,1) to a factor in [1 - jitter, 1 + jitter). *)
    let factor = 1. -. jitter +. (rand01 *. 2. *. jitter) in
    base *. factor

(* Seed the PRNG once so successive processes do not share a jitter schedule. *)
let () = Random.self_init ()

let next_delay t =
  let base = base_delay t.policy t.attempt in
  t.attempt <- t.attempt + 1;
  apply_jitter ~base ~jitter:t.policy.jitter ~rand01:(Random.float 1.0)
