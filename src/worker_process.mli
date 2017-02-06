(** This interface is not intended to be used directly by users!

    The master process forks a worker process for each task that Parallel is asked to do.
    After forking, in the worker process, the master calls [Worker_process.run], which:

    1. accepts a single connection from its [control_socket].
    2. reads a single marshaled [To_worker.Run f] message over the connection.
    3. creates a hub with [control_socket] and supplies it to [f].
    4. when [f] returns, writes a single [From_worker] message with the result over
       the [control_socket] connection.
    5. exits.
*)

open! Core
open! Async

module To_worker : sig
  type ('a, 'b, 'c) t =
  | Run of Writer.buffer_age_limit option * (('a, 'b) Hub.t -> 'c Deferred.t)
end

module From_worker : sig
  type 'a t =
  | Result of 'a
  | Exn of string
end

val run
  :  control_socket:([`Passive], Socket.Address.Inet.t) Socket.t
  -> never_returns
