(* The master process is a child of the main process that is using the parallel library.
   It is responsible for the worker processes, which are in turn its children. *)

open! Core
open! Async
open! Import

(* Init will raise this if any worker machine in the cluster fails initialization. It is
   safe to catch this exception and call init again. *)
exception Error_initializing_worker_machine of string * exn [@@deriving sexp]

(** [init] initializes the system and creates the master process.  [master_init], if
    specified, is called in the master process and may be used for cleanup/initialization
    such as closing file descriptors.  [init] should be called before any threads are
    created.  If your program daemonizes, call [init] after you daemonize, but before you
    start the async scheduler.  [init] may only be called once.

    If [cluster] is specified, and it specifies a set of host names or ips that you can
    ssh to without a password, and have permission to run programs on, then a master
    process will also be started on these machines, and you will be able to spawn worker
    processes on these machines. The current machine is implicitly included in the set of
    available machines.
*)
val init :
  ?cluster:Cluster.t ->
  ?close_stdout_and_stderr:bool ->
  unit ->
  Pid.t (* of the master process *)

(** All the functions below are called in either the main process or a worker process. *)


(** Request a new process. Returns the ip/port where the new process may be reached. If
    specified [where] determines which machine the process will be spawned on, the default
    is `Local (the current machine). *)
val create_process :
  ?where:[`Local | `On of string | `F of (unit -> string)]
  -> unit
  -> (Unix.Inet_addr.t * int) Or_error.t Deferred.t

(** Tell the master process to shutdown. *)
val shutdown : unit -> unit Deferred.t

(* Process distribution methods *)
val round_robin : [> `F of (unit -> string)]
val random : [> `F of (unit -> string)]
val random_in : string list -> [> `F of (unit -> string)]
