(** A hub is a place to which any number (possibly zero) of clients can connect a channel
    and send messages.  The process in which the hub is created is responsible for
    listening to the messages clients send, and can send messages to an individual client,
    or broadcast a message to all clients.

    Unless otherwise noted none of the below functions may be called in a process other
    than the one that created the hub. *)

open! Core
open! Async
open! Import

module Client_id : sig
  type t [@@deriving sexp]
  include Comparable with type t := t
  include Hashable with type t := t
end

type ('from_client, 'to_client) t

val create : ?buffer_age_limit:Writer.buffer_age_limit
  -> ([`Passive], Socket.Address.Inet.t) Socket.t
  -> (_, _) t Deferred.t

(* Note, this will close the listening socket as well as all connected clients. *)
val close : (_, _) t -> unit Deferred.t

(** [listen] and [listen_simple] start a loop that accepts connections from clients
    that wish to open channels connected to the hub.  [listen_simple] returns the
    sequence of messages sent by clients.  [listen] returns those, intermixed with
    messages indicating when clients [`Connect] and [`Disconnect].

    [listen] or [listen_simple] should be called exactly once for a given hub.
    Subsequent calls will raise. *)
val listen
  :  ('a, _) t
  -> [ `Connect of Client_id.t
     | `Disconnect of Client_id.t * string
     | `Data of Client_id.t * 'a
     ] Pipe.Reader.t

val listen_simple : ('a, _) t -> (Client_id.t * 'a) Pipe.Reader.t

val send        : (_, 'a) t -> Client_id.t -> 'a -> unit
val send_to_all : (_, 'a) t ->                'a -> unit
val flushed : (_, _) t -> unit Deferred.t

val clients : (_, _) t -> Client_id.t list

(** open_channel may be called even in a different process than the creator of the hub. *)
val open_channel : ('a, 'b) t -> ('a, 'b) Channel.t Deferred.t
val socket : (_, _) t -> Unix.Inet_addr.t * int
