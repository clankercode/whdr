type t =
  | Auth
  | Http of int
  | Revoked
  | Handler of exn
  | Cursor_store of exn
  | Transport of string
  | Connection_closed
  | Request of string

let is_fatal = function
  | Auth | Revoked | Handler _ | Cursor_store _ | Request _ -> true
  | Http _ | Transport _ | Connection_closed -> false

let to_string = function
  | Auth -> "authentication failed (HTTP 401): token missing, wrong, or revoked"
  | Http code -> Printf.sprintf "websocket upgrade failed with HTTP %d" code
  | Revoked -> "connection closed by server: token revoked"
  | Handler exn -> "event handler failed: " ^ Printexc.to_string exn
  | Cursor_store exn -> "cursor store failed: " ^ Printexc.to_string exn
  | Transport msg -> "websocket transport error: " ^ msg
  | Connection_closed -> "connection closed"
  | Request msg -> "invalid connection request: " ^ msg
