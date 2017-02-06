(** A [Channel.t] is a bi-directional communication channel for communicating to a
    [Hub.t].  Channels are portable across processes.  A channel can be sent to another
    process, either explicitly or by being in a closure and it will continue to work. *)

open! Core
open! Async
open! Import

type ('to_hub, 'from_hub) t

(** [create] is type-unsafe, and should not be used by user code. *)
val create
  :  ?buffer_age_limit:[ `At_most of Time.Span.t | `Unlimited ]
  -> addr:Unix.Inet_addr.t * int
  -> unit
  -> (_, _) t Deferred.t
val close     : (_ , _ ) t -> unit Deferred.t
val read      : (_ , 'b) t -> 'b Deferred.t
val read_full : (_ , 'b) t -> 'b Reader.Read_result.t Deferred.t
val write     : ('a, _ ) t -> 'a -> unit

type 'a pre_packed
val pre_pack : 'a -> 'a pre_packed
val write_pre_packed : ('a, _) t -> 'a pre_packed -> unit

val flushed    : (_, _) t -> unit Deferred.t
val socket     : (_, _) t -> Unix.Inet_addr.t * int

(** Similar to [Monitor.detach_and_get_error_stream], collects all writer errors. If this function has never
    been called, then exceptions will be raised directly *)
val errors : (_, _) t -> exn Stream.t
