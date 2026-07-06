type t = {
  mutable cursor : int64;
  seen : (string, unit) Hashtbl.t;
  order : string Queue.t;
  capacity : int;
}

let create ~cursor ~capacity =
  { cursor; seen = Hashtbl.create 256; order = Queue.create (); capacity = max 1 capacity }

let cursor t = t.cursor
let should_process t ~id ~seq = Int64.compare seq t.cursor > 0 && not (Hashtbl.mem t.seen id)

let record t ~id ~seq =
  if not (Hashtbl.mem t.seen id) then begin
    Hashtbl.replace t.seen id ();
    Queue.push id t.order;
    if Queue.length t.order > t.capacity then begin
      let old = Queue.pop t.order in
      Hashtbl.remove t.seen old
    end
  end;
  if Int64.compare seq t.cursor > 0 then t.cursor <- seq
