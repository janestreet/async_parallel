open! Core
open! Async
open! Import

(** [init] initializes the system and creates the master process.  [master_init], if
    specified, is called in the master process and may be used for cleanup/initialization
    such as closing file descriptors.  [init] should be called before any threads are
    created.  If your program daemonizes, call [init] after you daemonize, but before you
    start the async scheduler.  [init] may only be called once.

    If [cluster] is specified, and it specifies a set of host names or ips that you can
    ssh to without a password, and have permission to run programs on, then a master
    process will also be started on every machine in the cluster, and worker processes may
    be spawned on any machine in the cluster. Note that in ord
*)
val init : ?cluster:Cluster.t
  (** Because [init] forks, it is very hard to reason about your program if anything
      asyncy has already happened.  So, by default, [Parallel.init] fails if you call it
      after doing anything asyncy (defined as "have created the async scheduler").  You
      can override this behavior, but it would be much better to change your program to
      call [init] earlier.

      [~close_stdout_and_stderr] will close [stdout] and [stderr] in the master process.

      [~fail_if_async_has_been_initialized] exists only because of code that existed prior
      to [init] checking whether the Async scheduler has been initialized.  That old code
      uses [~fail_if_async_has_been_initialized:false].  All new code should not supply
      [~fail_if_async_has_been_initialized] and should accept the default. *)
  -> ?close_stdout_and_stderr : bool (* default is false *)
  -> ?fail_if_async_has_been_initialized : bool (* default is true *)
  -> unit
  -> unit

(** [shutdown] requests that the master process kill all workers and then shutdown.  It
    then waits for the master process to exit.  [shutdown] returns [Ok ()] when the master
    exits without problems; otherwise it returns an error. *)
val shutdown : unit -> unit Or_error.t Deferred.t



(** Run the specified closure in another process and return its result.

    If [where] is specified, it controls which machine the process is spawned on. The
    default is the local machine. You must have passed a list of machines to init in order
    to use `On, or `Random_on. An exception will be raised if you try to use a machine you
    didn't pass to init.

    The closure you pass may not contain custom blocks with unimplemented serialization
    functions or Abstract values. Anything you can't pass to Marshal, you can't pass to
    spawn.
*)
val run
  : ?buffer_age_limit:[ `At_most of Time.Span.t | `Unlimited ]
  -> ?where:[`Local | `On of string | `F of (unit -> string)]
  -> (unit -> 'a Deferred.t)
  -> ('a, string) Result.t Deferred.t

(** [spawn f] spawns a process running [f], supplying [f] a hub that it may use to
    communicate with other processes.  [f] should listen to the hub to receive messages
    from the clients.  [spawn] returns a channel connected to [f]'s hub, and a deferred
    that will become determined if [f] returns.

    There is no guarantee that the deferred returned by this function will become
    determined before the spawned process runs, as such the following code is a race, and
    may never return.

    |  spawn (fun hub -> Hub.send_to_all hub `Hello; Deferred.never ())
    |  >>= fun (channel, _) ->
    |  Channel.read channel
    |  >>= fun `Hello ->
    |  ...

    It IS however guaranteed that the spawned process is listening when the deferred
    returned by this function is returned, it is theirfore recommended that the spawning
    process initiate the first communication.

    If [where] is specified, it controls which machine the process is spawned on. The
    default is the local machine. You must have passed a list of machines to init in order
    to use `On, or `Random_on. An exception will be raised if you try to use a machine you
    didn't pass to init.

    The closure you pass may not contain custom blocks with unimplemented serialization
    functions or Abstract values. Anything you can't pass to Marshal, you can't pass to
    spawn.
*)
val spawn
   : ?buffer_age_limit:Writer.buffer_age_limit
  -> ?where:[`Local | `On of string | `F of (unit -> string)]
  -> (('a, 'b) Hub.t -> 'c Deferred.t)
  -> (('a, 'b) Channel.t * ('c, string) Result.t Deferred.t) Deferred.t

(** create a new hub. *)
val hub : ?buffer_age_limit:Writer.buffer_age_limit
  -> unit
  -> (_, _) Hub.t Deferred.t

(** returns true if this is a worker machine. See the notes on running on multiple
    machines in Std.ml. *)
val is_worker_machine : unit -> bool

(* Process distribution methods *)

(* Will spread processes evenly over cluster machines in a round robin way, each spawn
   will go to a different cluster machine. Of course its state is local to the current
   process, so in a new worker process it will always put a worker created from that
   machine on the first host. As a result random may be a better choice for multi level
   programs (workers creating workers in a tree like structure) because it does
   Random.State.make_self_init in each process. *)
val round_robin : [> `F of (unit -> string)]

(* Will pick a random cluster machine. If you have a lot of jobs, this will result in a
   pretty even distribution (and is useful for testing that your program isn't machine
   dependent. *)
val random : [> `F of (unit -> string)]

(* Will pick a random cluster machine in the specified subset of machines. *)
val random_in : string list -> [> `F of (unit -> string)]
