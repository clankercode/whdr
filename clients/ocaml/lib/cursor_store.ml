type t = { load : unit -> int64 Lwt.t; save : int64 -> unit Lwt.t }

let memory initial =
  let cell = ref initial in
  {
    load = (fun () -> Lwt.return !cell);
    save =
      (fun c ->
        cell := c;
        Lwt.return_unit);
  }
